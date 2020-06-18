package de.saar.minecraft.analysis;

import static de.saar.minecraft.broker.db.Tables.GAMES;
import static de.saar.minecraft.broker.db.Tables.GAME_LOGS;
import static java.time.temporal.ChronoUnit.SECONDS;


import de.saar.minecraft.broker.db.Tables;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;


public class GameInformation {
    int gameId;
    DSLContext jooq;

    private static final Logger logger = LogManager.getLogger(GameInformation.class);

    public GameInformation(int gameId, DSLContext jooq) {
        this.gameId = gameId;
        this.jooq = jooq;
    }

    public String getScenario() {
        return jooq.select(GAMES.SCENARIO)
            .from(GAMES)
            .where(GAMES.ID.eq(gameId))
            .fetchOne(GAMES.SCENARIO);
    }

    public String getArchitect() {
        return jooq.select(GAMES.ARCHITECT_INFO)
            .from(GAMES)
            .where(GAMES.ID.eq(gameId))
            .fetchOne(GAMES.ARCHITECT_INFO);
    }

    public int getNumBlocksPlaced() {
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.MESSAGE_TYPE.eq("BlockPlacedMessage"))
            .fetchOne(0, int.class);
    }

    public int getNumBlocksDestroyed() {
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.MESSAGE_TYPE.eq("BlockDestroyedMessage"))
            .fetchOne(0, int.class);
    }

    public int getNumMistakes() {
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.MESSAGE.contains("Not there! please remove that block again"))
            .fetchOne(0, int.class);
    }

    public List<Pair<String, Integer>> getNumericQuestions() {
        return jooq.selectFrom(Tables.QUESTIONNAIRES)
                .where(Tables.QUESTIONNAIRES.GAMEID.equal(gameId))
                .orderBy(Tables.QUESTIONNAIRES.ID.asc())
                .fetchStream()
                .filter((row) -> NumberUtils.isDigits(row.getAnswer()))
                .map((row) -> new Pair<>(row.getQuestion(), Integer.parseInt(row.getAnswer())))
                .collect(Collectors.toList());
    }

    public List<Pair<String, String>> getFreeformQuestions() {
        return jooq.selectFrom(Tables.QUESTIONNAIRES)
                .where(Tables.QUESTIONNAIRES.GAMEID.equal(gameId))
                .orderBy(Tables.QUESTIONNAIRES.ID.asc())
                .fetchStream()
                .filter((row) -> ! NumberUtils.isDigits(row.getAnswer()))
                .map((row) -> new Pair<>(row.getQuestion(), row.getAnswer()))
                .collect(Collectors.toList());
    }

    /**
     *
     * @return True if the game was successfully finished, false if stopped early
     */
    public boolean wasSuccessful() {
        var selection = jooq.select()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.MESSAGE.contains("\"newGameState\": \"SuccessfullyFinished\""))
            .fetch();
        return (!selection.isEmpty());
    }

    /**
     *
     * @return the time between the experiment start and successfully completing the building
     */
    public long getTimeToSuccess() {
        assert wasSuccessful();
        var timeFinished = getSuccessTime();
        if (timeFinished == null) {
            throw new AssertionError("Can't measure time to success without success");
        }
        var timeStarted = getStartTime();
        return timeStarted.until(timeFinished, SECONDS);
    }

    /**
     * Returns the duration until the user logged out.  Note: This may be much longer than
     * until task completion!
     * @return Seconds elapsed between login and logout
     */
    public long getTotalTime() {
        var timeStarted = getStartTime();
        var timeFinished = getEndTime();
        return timeStarted.until(timeFinished, SECONDS);
    }

    /**
     *
     * @return the first Timestamp of the game
     */
    public LocalDateTime getStartTime() {
        return jooq.select(GAME_LOGS.TIMESTAMP)
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .orderBy(GAME_LOGS.ID.asc())
            .fetchAny(GAME_LOGS.TIMESTAMP);
    }

    /**
     *
     * @return the Timestamp when the game state changed to SuccessfullyFinished
     */
    public LocalDateTime getSuccessTime() {
        assert wasSuccessful();
        return jooq.select(GAME_LOGS.TIMESTAMP)
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.MESSAGE.contains("\"newGameState\": \"SuccessfullyFinished\""))
            .fetchOne(GAME_LOGS.TIMESTAMP);
    }

    /**
     *
     * @return the last Timestamp of the game
     */
    public LocalDateTime getEndTime() {
        return jooq.select(GAME_LOGS.TIMESTAMP)
            .from(GAME_LOGS)
            .where(Tables.GAME_LOGS.GAMEID.equal(gameId))
            .orderBy(Tables.GAME_LOGS.ID.desc())
            .fetchAny(GAME_LOGS.TIMESTAMP);
    }

    /**
     * Returns the times in ms between each block placed event (regardless of the instructions).
     */
    public List<Integer> getBlockPlacedDurations() {
        return null;
    }
    /**
     * Returns the instructions given to the user and the time in ms it took the user to complete
     * this instruction, e.g. [("wall", 8000), ("wall", 5000), ...] or [("block", 300), ... ("wall", 4000)]
     * (This is low prio for now, the other two are more important)
     */
    public List<Pair<String, Integer>> getDurationPerInstruction() {
        return null;
    }

    /**
     * Returns the time the user needed to build each HLO in the scenario, i.e. the walls
     * in the house scenario and the floor and railings in the bridge scenario.
     * e.g. [("wall", 8000), ("wall", 5000), ...] or [("floor", 3000), ... ("railing", 4000)]
     * Regardless of whether it was instructed per block or as a HLO.
     */
    public List<Pair<String, Integer>> getDurationPerHLO() {return null;}
}
