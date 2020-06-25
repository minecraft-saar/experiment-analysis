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
import java.util.List;
import java.util.stream.Collectors;

import de.saar.minecraft.broker.db.tables.records.GameLogsRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;


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
            .and(GAME_LOGS.MESSAGE_TYPE.eq("BlockPlacedMessage"))
            .and(GAME_LOGS.TIMESTAMP.lessOrEqual(getSuccessTime()))
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
            .and(GAME_LOGS.MESSAGE_TYPE.eq("BlockDestroyedMessage"))
            .and(GAME_LOGS.TIMESTAMP.lessOrEqual(getSuccessTime()))
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
                if (newInstruction.contains("Great! now") || newInstruction.contains("Congratulations")) {
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
        List<Pair<String, Integer>> durations = new ArrayList<>();
        List<Pair<String, Integer>> instructions = getDurationPerInstruction();
        logger.info("arch {}", getArchitect());
        logger.info("scenario {}", getScenario());

        if (getArchitect() == null) {
            return List.of();
        }
        if (getArchitect().endsWith("HIGHLEVEL")) {
            for (Pair<String, Integer> inst: instructions) {
                for (String hlo: List.of("wall", "row", "floor", "railing")) {
                    if (inst.getFirst().contains(hlo)) {
                        durations.add(new Pair<>(hlo, inst.getSecond()));
                        break;
                    }
                }
            }
        } else if (getArchitect().endsWith("MEDIUM")) {
            if (getScenario().equals("house")) {
                //wall: 1. 6 blocks, 2. - 4: wall
                // row: 1. 4 blocks, 2. - 4.: row
                if (instructions.size() < 16) {
                    logger.error("Not enough blocks for game " + gameId + ": " + instructions.size());
                    return List.of();
                }
                durations.add(new Pair<>("wall", instructions.subList(0, 6).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("wall", instructions.get(6).getSecond()));
                durations.add(new Pair<>("wall", instructions.get(7).getSecond()));
                durations.add(new Pair<>("wall", instructions.get(8).getSecond()));

                durations.add(new Pair<>("row", instructions.subList(9, 13).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("row", instructions.get(13).getSecond()));
                durations.add(new Pair<>("row", instructions.get(14).getSecond()));
                durations.add(new Pair<>("row", instructions.get(15).getSecond()));

            } else if (getScenario().equals("bridge")) {
                // floor: 13 blocks
                // railing: 1. + 2. 7 blocks
                if (instructions.size() < 21) {
                    logger.error("Not enough blocks in game {}: {}", gameId, instructions.size());
                    return List.of();
                }
                durations.add(new Pair<>("floor", instructions.subList(0, 13).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("railing", instructions.subList(13, 20).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("railing", instructions.get(20).getSecond()));
            } else {
                throw new NotImplementedException("Unknown scenario: " + getScenario());
            }

        } else if (getArchitect().endsWith("BLOCK")) {
            if (getScenario().equals("house")) {
                //wall: 1. 6 blocks, 2. 5 blocks, 3. 5 blocks, 4. 4 blocks
                // row: 1. - 4. 4 blocks
                if (instructions.size() < 36) {
                    logger.error("Not enough blocks for game " + gameId + ": " + instructions.size());
                    return List.of();
                }
                durations.add(new Pair<>("wall", instructions.subList(0, 6).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("wall", instructions.subList(6, 11).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("wall", instructions.subList(11, 16).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("wall", instructions.subList(16, 20).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));

                durations.add(new Pair<>("row", instructions.subList(20, 24).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("row", instructions.subList(24, 28).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("row", instructions.subList(28, 32).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("row", instructions.subList(32, 36).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));

            } else if (getScenario().equals("bridge")) {
                // floor: 13 blocks
                // railing: 1. + 2. 7 blocks
                if (instructions.size() < 27) {
                    logger.error("Not enough blocks in game {}: {}", gameId, instructions.size());
                    return List.of();
                }
                durations.add(new Pair<>("floor", instructions.subList(0, 13).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("railing", instructions.subList(13, 20).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
                durations.add(new Pair<>("railing", instructions.subList(20, 27).stream().mapToInt(Pair::getSecond).reduce(0, Integer::sum)));
            } else {
                throw new NotImplementedException("Unknown scenario: " + getScenario());
            }
        } else if (getArchitect().equals("DummyArchitect")) {
            return List.of();
        } else {
            throw new NotImplementedException("Unknown architect: " + getArchitect());
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
