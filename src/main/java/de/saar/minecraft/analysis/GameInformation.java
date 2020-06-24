package de.saar.minecraft.analysis;

import static de.saar.minecraft.broker.db.Tables.GAMES;
import static de.saar.minecraft.broker.db.Tables.GAME_LOGS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.saar.minecraft.broker.db.GameLogsDirection;
import de.saar.minecraft.broker.db.Tables;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import de.saar.minecraft.broker.db.tables.records.GameLogsRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.JSON;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.tools.json.JSONObject;


public class GameInformation {
    int gameId;
    DSLContext jooq;
    /** The ID of the log message showing that the user was successful.*/
    final long successMessageID;

    private static final Logger logger = LogManager.getLogger(GameInformation.class);

    public GameInformation(int gameId, DSLContext jooq) {
        this.gameId = gameId;
        this.jooq = jooq;
        Long mid = jooq.select(GAME_LOGS.ID)
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.MESSAGE.contains("\"newGameState\": \"SuccessfullyFinished\""))
            .fetchOne(GAME_LOGS.ID);
        if (mid == null) {
            successMessageID = Long.MAX_VALUE;
        } else {
            successMessageID = mid;
        }
    }

    public  String getCSVHeader(String separator) {
        var sb = new StringBuilder();
        int qnum = 0;
        for (var qa: getNumericQuestions()
                .stream()
                .sorted(Comparator.comparing(Pair::getFirst))
                .collect(Collectors.toList())) {
            sb.append("# Question"+qnum+": ")
                    .append(qa.getFirst())
                    .append("\n");
            qnum += 1;
        }
        sb.append("scenario")
          .append(separator)
          .append("architect")
          .append(separator)
          .append("wasSuccessful")
          .append(separator)
          .append("timeToSuccess")
          .append(separator)
          .append("numBlocksPlaced")
          .append(separator)
          .append("numBlocksDestroyed")
          .append(separator)
          .append("numMistakes");

        for (int i = 0; i < 8; i++) {
            sb.append(separator);
            sb.append("HLO").append(i);
        }

        for (int i = 0; i < getNumericQuestions().size(); i++) {
            sb.append(separator).append("Question").append(i);
        }
        return sb.append("\n").toString();
    }
    /**
     * Returns Scenario Architect wassucessful timetosuccess numblocksplaced numblocksdestroyed nummistakes answers
     * @param separator the separator of the fields
     */
    public String getCSVLine(String separator) {
        var sb = new StringBuilder()
                .append(getScenario())
                .append(separator)
                .append(getArchitect())
                .append(separator)
                .append(wasSuccessful())
                .append(separator);
        try { // only a valid time if actually successful
            sb.append(getTimeToSuccess());
        } catch (AssertionError e) {
            sb.append("NA");
        }
        sb.append(separator)
                 .append(getNumBlocksPlaced())
                 .append(separator)
                 .append(getNumBlocksDestroyed())
                 .append(separator)
                 .append(getNumMistakes());

        var hloTimings = getDurationPerHLO();
        for (int i = 0; i < 8; i++) {
            sb.append(separator);
            if (i < hloTimings.size()) {
                sb.append(hloTimings.get(i).getSecond());
            } else {
                sb.append("NA");
            }
        }

        getNumericQuestions().stream().sorted(Comparator.comparing(Pair::getFirst)).forEach(
                (x) -> {
                    sb.append(separator);
                    sb.append(x.getSecond());
                }
        );
        return sb.append("\n").toString();
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

    public String getPlayerName() {
        return jooq.select(GAMES.PLAYER_NAME)
                .from(GAMES)
                .where(GAMES.ID.equal(gameId))
                .fetchOne(GAMES.PLAYER_NAME);
    }

    public String getClientIp() {
        return jooq.select(GAMES.CLIENT_IP)
                .from(GAMES)
                .where(GAMES.ID.equal(gameId))
                .fetchOne(GAMES.CLIENT_IP);
    }

    /**
     *
     * @return number of blocks placed before the experiment was successful
     */
    public int getNumBlocksPlaced() {
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.ID.lessOrEqual(successMessageID))
            .and(GAME_LOGS.MESSAGE_TYPE.eq("BlockPlacedMessage"))
            .fetchOne(0, int.class);
    }

    /**
     *
     * @return number of blocks destroyed before the experiment was successful
     */
    public int getNumBlocksDestroyed() {
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.ID.lessOrEqual(successMessageID))
            .and(GAME_LOGS.MESSAGE_TYPE.eq("BlockDestroyedMessage"))
            .fetchOne(0, int.class);
    }

    public int getNumMistakes() {
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.ID.lessOrEqual(successMessageID))
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
        return successMessageID < Long.MAX_VALUE;
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
        Result<Record1<LocalDateTime>> result = jooq.select(GAME_LOGS.TIMESTAMP)
                .from(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .and(GAME_LOGS.MESSAGE_TYPE.equal("BlockPlacedMessage"))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();


        List<Integer> durations = new ArrayList<>();
        if (result.isNotEmpty()) {
            // add duration until first block placed
            durations.add((int) getStartTime().until(result.getValue(0, GAME_LOGS.TIMESTAMP), MILLIS));
            for (int i = 1; i < result.size(); i++) {
                durations.add((int) result.getValue(i - 1, GAME_LOGS.TIMESTAMP)
                        .until(result.getValue(i, GAME_LOGS.TIMESTAMP), MILLIS));
            }
        }
        return durations;
    }

    /**
     * Returns the instructions given to the user and the time in ms it took the user to complete
     * this instruction, e.g. [("wall", 8000), ("wall", 5000), ...] or [("block", 300), ... ("wall", 4000)]
     * (This is low prio for now, the other two are more important)
     */
    public List<Pair<String, Integer>> getDurationPerInstruction() {
        List<Pair<String, Integer>> durations = new ArrayList<>();
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();
        String oldInstruction = null;
        LocalDateTime oldTimestamp = null;
        for (GameLogsRecord record: result) {
            if (record.getDirection().equals(GameLogsDirection.PassToClient)
                    && record.getMessageType().equals("TextMessage")) {
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (! jsonObject.has("text")) {
                    continue;
                }
                String newInstruction = jsonObject.get("text").getAsString();
                if (newInstruction.contains("Great! now")) {
                    if (oldInstruction == null) {
                        oldInstruction = newInstruction;
                        oldTimestamp = record.getTimestamp();
                        continue;
                    }
                    int duration = (int)oldTimestamp.until(record.getTimestamp(), MILLIS);
                    durations.add(new Pair<>(oldInstruction, duration));
                    oldInstruction = newInstruction;
                    oldTimestamp = record.getTimestamp();
                }
            }
        }

        return durations;
    }

    /**
     * Returns the time the user needed to build each HLO in the scenario, i.e. the walls
     * in the house scenario and the floor and railings in the bridge scenario.
     * e.g. [("wall", 8000), ("wall", 5000), ...] or [("floor", 3000), ... ("railing", 4000)]
     * Regardless of whether it was instructed per block or as a HLO.
     */
    public List<Pair<String, Integer>> getDurationPerHLO() {
        assert wasSuccessful();
        List<Pair<String, Integer>> durations;
        List<GameLogsRecord> correctBlocks = getCorrectBlocks();
        if (getScenario().equals("house")) {
            // wall: 1. 6 blocks, 2. 5 blocks, 3. 5 blocks, 4. 4 blocks
            // row: 1. - 4. 4 blocks
            if (correctBlocks.size() < 36) {
                logger.error("Not enough blocks in game {}", gameId);
                return List.of();
            }
            long firstWall = getStartTime().until(correctBlocks.get(5).getTimestamp(), MILLIS);
            long secondWall = correctBlocks.get(5).getTimestamp().until(correctBlocks.get(10).getTimestamp(), MILLIS);
            long thirdWall = correctBlocks.get(10).getTimestamp().until(correctBlocks.get(15).getTimestamp(), MILLIS);
            long fourthWall = correctBlocks.get(15).getTimestamp().until(correctBlocks.get(19).getTimestamp(), MILLIS);

            long firstRow = correctBlocks.get(19).getTimestamp().until(correctBlocks.get(23).getTimestamp(), MILLIS);
            long secondRow = correctBlocks.get(23).getTimestamp().until(correctBlocks.get(27).getTimestamp(), MILLIS);
            long thirdRow = correctBlocks.get(27).getTimestamp().until(correctBlocks.get(31).getTimestamp(), MILLIS);
            long fourthRow = correctBlocks.get(31).getTimestamp().until(correctBlocks.get(35).getTimestamp(), MILLIS);
            durations = List.of(
                    Pair.create("wall", (int)firstWall),
                    Pair.create("wall", (int)secondWall),
                    Pair.create("wall", (int)thirdWall),
                    Pair.create("wall", (int)fourthWall),
                    Pair.create("row", (int)firstRow),
                    Pair.create("row", (int)secondRow),
                    Pair.create("row", (int)thirdRow),
                    Pair.create("row", (int)fourthRow)
            );
        } else if (getScenario().equals("bridge")) {
            // floor: 13 blocks
            // railing: 1. + 2. 7 blocks
            if (correctBlocks.size() < 27) {
                logger.error("Not enough blocks in game {}", gameId);
                return List.of();
            }
            long floor = getStartTime().until(correctBlocks.get(12).getTimestamp(), MILLIS);
            long firstRailing = correctBlocks.get(12).getTimestamp().until(correctBlocks.get(19).getTimestamp(), MILLIS);
            long secondRailing = correctBlocks.get(19).getTimestamp().until(correctBlocks.get(26).getTimestamp(), MILLIS);
            durations = List.of(
                    Pair.create("floor", (int)floor),
                    Pair.create("railing", (int)firstRailing),
                    Pair.create("railing", (int)secondRailing)
            );
        } else {
            throw new NotImplementedException("Unknown scenario " + getScenario());
        }
        return durations;
    }

    /**
     * TODO: does not work if the player placed two or more blocks before the architect reacts with a correction.
     * @return game logs for all correct blocks
     */
    public List<GameLogsRecord> getCorrectBlocks() {
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();
        List<GameLogsRecord> blocks = new ArrayList<>();
        for (GameLogsRecord record: result) {
            if (record.getMessageType().equals("BlockPlacedMessage")) {
                blocks.add(record);
            } else if (record.getMessage().contains("Not there! please remove that block again")) {
                blocks.remove(blocks.size() - 1);
            }
        }
        return blocks;
    }

    public void writeAnalysis(File file) throws IOException {
        FileWriter writer = new FileWriter(file);
        boolean wasSuccessful = wasSuccessful();
        String overview = "# Overview" +
                "\n - Connection from: " +
                getClientIp() +
                "\n - Player name: " +
                getPlayerName() +
                "\n - Scenario: " +
                getScenario() +
                "\n - Architect: " +
                getArchitect() +
                "\n - Successful: " +
                wasSuccessful +
                "\n\n## Times\n" +
                "\n - Start Time: " +
                getStartTime() +
                "\n - Success Time: " +
                (wasSuccessful ? getSuccessTime() : "not applicable") +
                "\n - End Time: " +
                getEndTime() +
                "\n - Experiment Duration: " +
                (wasSuccessful ? getTimeToSuccess() + " seconds" : "not applicable") +
                "\n - Total time logged in: " +
                getTotalTime() +
                "seconds\n\n## Blocks\n" +
                "\n - Number of blocks placed: " +
                getNumBlocksPlaced() +
                "\n - Number of blocks destroyed: " +
                getNumBlocksDestroyed() +
                "\n - Number of mistakes: " +
                getNumMistakes();
        writer.write(overview);
        writer.flush();

        List<Integer> blockDurations = getBlockPlacedDurations();
        StringBuilder durations = new StringBuilder("\n\n# Duration per block");
        for (int current: blockDurations) {
            durations.append("\n - ").append(current).append("ms");
        }

        List<Pair<String, Integer>> HLODurations = getDurationPerHLO();
        durations.append("\n\n# Durations per High-level object");
        for (var pair: HLODurations) {
            durations.append("\n - ").append(pair.getFirst());
            durations.append(" : ").append(pair.getSecond());
            durations.append("ms");
        }

        List<Pair<String, Integer>> instructionDurations = getDurationPerInstruction();
        durations.append("\n\n# Durations per Instruction");
        for (var pair: instructionDurations) {
            durations.append("\n - ").append(pair.getFirst());
            durations.append(" : ").append(pair.getSecond());
            durations.append("ms");
        }
        writer.write(durations.toString());
        writer.close();

    }
}
