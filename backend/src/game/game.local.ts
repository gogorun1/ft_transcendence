import { AppError, ErrorCode } from "../error/apperror";
import { QuestionService } from "../question/question.service";
import { Room } from "../room/room.types";
import { GameBaseService } from "./game.base";
import { RedisGameRepository } from "./game.redis.repository";
import { BaseGameState, MultiGameState, Player } from "./game.types";
import { GameMode } from "@prisma/client";
import { AIService } from "./ai";

type LastAnswerUpdate = {
    playerId: string;
    isCorrect: boolean;
    correctAnswerIndex: number;
    correctText: string;
};

type AtomicAnswerTask = {
    id: string;
    ans: number;
    questionId: number;
    visibleAt?: number;
};

/**
 * LocalMultiPlayer
 * 负责本地房间向游戏状态机的初始化转换，以及驱动 Redis Lua 沙盒完成双人/多人原子答题结算。
 * 遵循 方案 3：人类交卷时现场秒杀 AI 逻辑，不产生任何异步 Timer 竞争。
 */
export class LocalMultiPlayer extends GameBaseService {
    constructor(
        questionservice: QuestionService,
        protected readonly gamerepository: RedisGameRepository,
        private readonly aiservice: AIService,
    ){
        super(questionservice);
    }

    /**
     * 将匹配成功的 Lobby Room 转换为分布式内存中的运行时 Live GameState 快照。
     */
    async startGame(room: Room, category?: string): Promise<BaseGameState> {
        const playerlist = Object.values(room.players);

        // 核心防御：联机对战必须满足至少 2 个活跃连接
        if (playerlist.length < 2) {
            throw new AppError('Not enough players', ErrorCode.ROOM_PLAYER_NBR, 400);
        }

        const players: Record<string, Player> = {};
        let hasAIInRoom = false;

        // 解包房间玩家，构建包含虚拟 AI 标签的游戏玩家档案
        for (const p of playerlist) {
            const userIdstring = String(p.userId);
            const isAIPlayer = userIdstring.startsWith("ai_");
            if (isAIPlayer) {
                hasAIInRoom = true;
            }
            
            players[userIdstring] = {
                ...this.initPlayers(userIdstring, p.nickname),
                isAI: isAIPlayer
            };
        }

        let state: MultiGameState;

        // 分流解析：根据房间属性决定当前生成的是 AI 对战房间还是纯人类联机房间
        if (hasAIInRoom) {
            state = await this.prepareGame(players, "AI" as GameMode, { roomId: room.roomId, hostId: room.hostId, category }) as MultiGameState;
        } else {
            state = await this.prepareGame(players, "MULTIPLAYER", { roomId: room.roomId, hostId: room.hostId, category }) as MultiGameState;
        }

        console.log("[Game Init Local] Successfully initialized state for room:", state.roomId);

        // 锦标赛房间上下文穿透
        if (room.tournamentId) {
            state.tournamentId = room.tournamentId;
        }

        // 完美序列化存入 Redis 内存区
        await this.gamerepository.create(state);
        return state;
    }

    /**
     * 答题管道唯一入口。
     * 方案 3 核心演进：在击打 Lua 脚本之前，预先判定并完成 AI 决策的同步装载。
     */
    async submitAnswer(gameId: string, selectedAnswerIndex: number, userId: string): Promise<{
        state: BaseGameState;
        lastAnswer: LastAnswerUpdate;
    }> {
        console.log(`[submit] game=${gameId}, user=${userId}, answer=${selectedAnswerIndex}`);

        // 1. 提取当前第一线快照，用以计算 AI 是否需要“悄悄做预判”
        const currentGameState = await this.gamerepository.findById(gameId);
        if (!currentGameState) {
            throw new AppError('Game not found', ErrorCode.GAME_NOT_FOUND, 404);
        }
        const playerId = String(userId);
        const currentQuestion = currentGameState.questions[currentGameState.currentQuestionIndex];
        if (!currentQuestion) {
            throw new AppError('Question not found', ErrorCode.QUESTION_NOT_FOUND, 404);
        }

        const tasks: AtomicAnswerTask[] = [{
            id: playerId,
            ans: selectedAnswerIndex,
            questionId: currentQuestion.id,
        }];
        const isAIGame = currentGameState.mode === 'AI';

        // 🌟 方案 3 核心注入点：如果当前是 AI 模式且游戏未完结
        if (isAIGame && !currentGameState.isFinished) {
            const aiId = Object.keys(currentGameState.players).find(id => currentGameState.players[id].isAI);
            const aiPlayer = aiId ? currentGameState.players[aiId] : null;

            // 只有当 AI 在当前这一题依然处于未答（playing）状态时，军师（AIService）才会现身出谋划策
            if (aiId && aiPlayer && aiPlayer.status === 'playing') {
                // 调用纯净的同步预测函数，算出 AI 答案和隐藏解封时间戳
                const aiPayload = this.aiservice.predictAnswer(currentGameState, aiId);
                if (aiPayload.questionId === currentQuestion.id) {
                    tasks.push({
                        id: aiPayload.aiId,
                        ans: aiPayload.selectedAnswerIndex,
                        questionId: aiPayload.questionId,
                        visibleAt: aiPayload.visibleAt,
                    });
                }
            }
        }

        // 2. 【关键修正】：将人类答案和 AI 预判答案作为统一 tasks 原子拍入 Lua 脚本
        const result = await this.gamerepository.submitanswerAtomic(
            gameId,
            tasks,
            Date.now()
        );

        // 3. 基础空值及状态未找到边界防御
        if (!result || result.error === "STATE_NOT_FOUND") {
            throw new AppError('Game not found', ErrorCode.GAME_NOT_FOUND, 404);
        }

        // 4. 精准识别 Lua 返回的冲突状态，返回给上层 GameService 充当后端内部交通信号灯
        if (result.error === "ALREADY_ANSWERED") {
            // 幂等性拦截：说明用户狂点了。duplicated 信号可以让上层直接短路，不重复做外部触发
            return {
                state: result.state,
                lastAnswer: {
                    playerId,
                    isCorrect: false,
                    correctAnswerIndex: currentQuestion.correctAnswerIndex,
                    correctText: 'ALREADY_PROCESSED'
                }
            };
        }
        if (result.error === "GAME_FINISHED" || result.error === "NO_QUESTION") {
            // 过期忽略拦截：说明答题卡秒踩中了切题临界点，忽略多余逻辑
            return {
                state: result.state,
                lastAnswer: {
                    playerId,
                    isCorrect: false,
                    correctAnswerIndex: currentQuestion.correctAnswerIndex,
                    correctText: 'ALREADY_PROCESSED'
                }
            };
        }

        // 5. 组装返回给上层单入口协调者（GameService）的元数据
        const state = result.state as BaseGameState;
        const roundResolved = state.isFinished
            || state.currentQuestionIndex !== currentGameState.currentQuestionIndex
            || Object.values(state.players)
                .filter(player => player.status !== 'disconnected')
                .every(player => player.status === 'answered');
        const correctText = currentQuestion.options[currentQuestion.correctAnswerIndex] ?? "";
        const lastAnswer = {
            playerId,
            isCorrect: selectedAnswerIndex === currentQuestion.correctAnswerIndex,
            correctAnswerIndex: roundResolved ? currentQuestion.correctAnswerIndex : -1,
            correctText: roundResolved ? correctText : "",
        };

        return { state, lastAnswer };
    }
}
