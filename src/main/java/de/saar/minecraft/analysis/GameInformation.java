package de.saar.minecraft.analysis;

import static de.saar.minecraft.broker.db.Tables.GAMES;
import static de.saar.minecraft.broker.db.Tables.GAME_LOGS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.saar.coli.minecraft.relationextractor.Block;
import de.saar.minecraft.broker.db.GameLogsDirection;
import de.saar.minecraft.broker.db.Tables;
import de.saar.minecraft.broker.db.tables.records.GameLogsRecord;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;


public class GameInformation {
    int gameId;
    DSLContext jooq;
    boolean countDestroyedAsMistake;
    /** The ID of the log message showing that the user was successful.*/
    final long successMessageID;

    private static final Logger logger = LogManager.getLogger(GameInformation.class);

    public GameInformation(int gameId, DSLContext jooq) {
        this.gameId = gameId;
        this.jooq = jooq;
        this.countDestroyedAsMistake = true;
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
            // TODO: escape commas in questions
            sb.append("# Question").append(qnum).append(": ")
                    .append(qa.getFirst())
                    .append("\n");
            qnum += 1;
        }
        sb.append("gameid")
          .append(separator)
          .append("scenario")
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
     * Returns gameid Scenario Architect wassucessful timetosuccess numblocksplaced
     * numblocksdestroyed nummistakes answers
     * @param separator the separator of the fields
     */
    public String getCSVLine(String separator) {
        var sb = new StringBuilder()
                .append(gameId)
                .append(separator)
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

        if (wasSuccessful()) {
            var hloTimings = getDurationPerHLO();
            for (int i = 0; i < 8; i++) {
                sb.append(separator);
                if (i < hloTimings.size()) {
                    sb.append(hloTimings.get(i).getSecond());
                } else {
                    sb.append("NA");
                }
            }
        } else {
            sb.append((separator + "NA").repeat(8));
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

    /**
     *
     * @return number of times the architect messages about a incorrectly placed block
     */
    public int getNumMistakes() {
        if (countDestroyedAsMistake) {
            return jooq.selectCount()
                    .from(GAME_LOGS)
                    .where(GAME_LOGS.GAMEID.eq(gameId))
                    .and(GAME_LOGS.ID.lessOrEqual(successMessageID))
                    .and(GAME_LOGS.MESSAGE.contains("Not there! please remove that block again")
                        .or(GAME_LOGS.MESSAGE.contains("Please add this block again.")))
                    .fetchOne(0, int.class);
        }
        return jooq.selectCount()
            .from(GAME_LOGS)
            .where(GAME_LOGS.GAMEID.eq(gameId))
            .and(GAME_LOGS.ID.lessOrEqual(successMessageID))
            .and(GAME_LOGS.MESSAGE.contains("Not there! please remove that block again"))
            .fetchOne(0, int.class);
    }

    /**
     *
     * @return List of question-answer pairs where the answer is a number
     */
    public List<Pair<String, Integer>> getNumericQuestions() {
        return jooq.selectFrom(Tables.QUESTIONNAIRES)
                .where(Tables.QUESTIONNAIRES.GAMEID.equal(gameId))
                .orderBy(Tables.QUESTIONNAIRES.ID.asc())
                .fetchStream()
                .filter((row) -> NumberUtils.isDigits(row.getAnswer()))
                .map((row) -> new Pair<>(row.getQuestion(), Integer.parseInt(row.getAnswer())))
                .collect(Collectors.toList());
    }

    /**
     *
     * @return List of question-answer pairs where the answer is not a number
     */
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
     * this instruction.
     * Each instruction is represented by the derivation tree of the IRTG,
     * the times are returned as milliseconds between giving the instruction and
     * the next instruction.
     *
     * Correction instructions (e.g. to remove a block again or put a block again that was removed)
     * are ignored; only instructions that have new=true in their metadata are handled.
     */
    public List<Pair<String, Integer>> getDurationPerInstruction() {
        List<Pair<String, Integer>> durations = new ArrayList<>();
        var query = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc());
        String oldInstruction = null;
        LocalDateTime oldTimestamp = null;
        for (GameLogsRecord record: query) {
            if (record.getDirection().equals(GameLogsDirection.PassToClient)
                    && record.getMessageType().equals("TextMessage")) {
                JsonObject messageJson = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (! messageJson.has("text")) {
                    continue;
                }

                if ("SuccessfullyFinished".equals(messageJson.get("newGameState").getAsString())) {
                    // we are done, record last timing and get out
                    int duration = (int) oldTimestamp.until(record.getTimestamp(), MILLIS);
                    durations.add(new Pair<>(oldInstruction, duration));
                    break;
                }

                String newInstruction = messageJson.get("text").getAsString();
                
                if (! newInstruction.startsWith("{")) {
                    // ignore messages that have no structured information
                    // those are e.g. welcome messages etc.
                    continue;
                }

                JsonObject instructionJson = JsonParser.parseString(newInstruction).getAsJsonObject();
                
                // also contains the "message" field but we do not need that here
                boolean isNew = instructionJson.get("new").getAsBoolean();
                String tree = instructionJson.get("tree").getAsString();

                if (isNew) {
                    if (oldInstruction == null) {
                        // first instruction, just save and continue
                        oldInstruction = tree;
                        oldTimestamp = record.getTimestamp();
                        continue;
                    }
                    int duration = (int) oldTimestamp.until(record.getTimestamp(), MILLIS);
                    durations.add(new Pair<>(oldInstruction, duration));
                    oldInstruction = tree;
                    oldTimestamp = record.getTimestamp();
                }
            }
        }
        return durations;
    }

    /**
     * Reads a Block*Message and returns a Block with the same coordinates
     * @param record: a GamesLogsRecord for a BlockPlaced- or BlockDestroyedMessage
     */
    private Block getBlockFromRecord(GameLogsRecord record) {
        JsonObject json = JsonParser.parseString(record.getMessage()).getAsJsonObject();
        // If Block logs are incomplete, the missing values are the default 0
        // This should only occur for games that were played before
        // TODO: date of setting up infrastructure commit 96b2fde on the server
        int x = 0, y = 0, z = 0;
        if (json.has("x")) {
            x = json.get("x").getAsInt();
        } else {
            logger.error("Missing x value for block at {}: {}", json, record.getTimestamp());
        }
        if (json.has("y")) {
            y = json.get("y").getAsInt();
        } else {
            logger.error("Missing y value for block at {}: {}", json, record.getTimestamp());
        }
        if (json.has("z")) {
            z = json.get("z").getAsInt();
        } else {
            logger.error("Missing z value for block at {}: {}", json, record.getTimestamp());
        }
        return new Block(x, y, z);
    }

    /**
     * Returns the time the user needed to build each HLO in the scenario, i.e. the walls
     * in the house scenario and the floor and railings in the bridge scenario.
     * e.g. [("wall", 8000), ("wall", 5000), ...] or [("floor", 3000), ... ("railing", 4000)]
     * Regardless of whether it was instructed per block or as a HLO.
     *
     * <p>The method assumes that all HLOs were built in the correct order. The duration for each
     * HLO starts when the previous HLO is finished (or for the first with the welcome message).</p>
     */
    public List<Pair<String, Integer>> getDurationPerHLO() {
        if (! wasSuccessful()) {
            return List.of();
        }
        if (getArchitect() == null) {
            return List.of();
        }
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();

        List<MutablePair<LocalDateTime, List<Block>>> hloPlans;
        Set<Block> presentBlocks;
        String scenario = getScenario();
        if (scenario.equals("house")) {
            hloPlans = readHighlevelPlan("/de/saar/minecraft/domains/house-highlevel.plan");
            presentBlocks = readInitialWorld("/de/saar/minecraft/worlds/house.csv");
        } else if (scenario.equals("bridge")) {
            hloPlans = readBlockPlan("/de/saar/minecraft/domains/bridge-block.plan");
            presentBlocks = readInitialWorld("/de/saar/minecraft/worlds/bridge.csv");
        } else {
            throw new NotImplementedException("Scenario {} is not implemented", scenario);
        }

        LocalDateTime firstInstructionTime = null;

        for (GameLogsRecord record: result) {
            switch (record.getMessageType()) {
                case "BlockPlacedMessage": 
                    presentBlocks.add(getBlockFromRecord(record));
                    break;
                case "BlockDestroyedMessage":
                    presentBlocks.remove(getBlockFromRecord(record));
                    break;
                case "TextMessage":
                    if (firstInstructionTime == null) {
                        // the first instruction has a derivation tree
                        if (record.getMessage().contains("\\\"tree\\\":")) {
                            firstInstructionTime = record.getTimestamp();
                        }
                    }
                    break;
                case "SuccessfullyFinished":
                    if (hloPlans.get(hloPlans.size() - 1).getLeft() == null) {
                        hloPlans.get(hloPlans.size() - 1).setLeft(record.getTimestamp());
                    }
                    break;
                default:
                    break;
            }
            // Check which HLOs are complete
            for (MutablePair<LocalDateTime, List<Block>> hlo : hloPlans) {
                if (hlo.getLeft() == null) {
                    if (presentBlocks.containsAll(hlo.getRight())) {
                        hlo.setLeft(record.getTimestamp());
                    }
                }
            }
        }

        if (firstInstructionTime == null) {
            logger.error("first instruction is null");
            return List.of();
        }
        if (getScenario().equals("bridge")) {
            return List.of(
                    new Pair<>("floor", (int) firstInstructionTime.until(hloPlans.get(0).getLeft(), MILLIS)),
                    new Pair<>("railing", (int) hloPlans.get(0).getLeft().until(hloPlans.get(1).getLeft(), MILLIS)),
                    new Pair<>("railing", (int) hloPlans.get(1).getLeft().until(hloPlans.get(2).getLeft(), MILLIS))
            );
        } else if (getScenario().equals("house")) {
            return List.of(
                    new Pair<>("wall", (int) firstInstructionTime.until(hloPlans.get(0).getLeft(), MILLIS)),
                    new Pair<>("wall", (int) hloPlans.get(0).getLeft().until(hloPlans.get(1).getLeft(), MILLIS)),
                    new Pair<>("wall", (int) hloPlans.get(1).getLeft().until(hloPlans.get(2).getLeft(), MILLIS)),
                    new Pair<>("wall", (int) hloPlans.get(2).getLeft().until(hloPlans.get(3).getLeft(), MILLIS)),
                    new Pair<>("row", (int) hloPlans.get(3).getLeft().until(hloPlans.get(4).getLeft(), MILLIS)),
                    new Pair<>("row", (int) hloPlans.get(4).getLeft().until(hloPlans.get(5).getLeft(), MILLIS)),
                    new Pair<>("row", (int) hloPlans.get(5).getLeft().until(hloPlans.get(6).getLeft(), MILLIS)),
                    new Pair<>("row", (int) hloPlans.get(6).getLeft().until(hloPlans.get(7).getLeft(), MILLIS))
            );
        }
        return List.of();
    }

    private List<MutablePair<LocalDateTime, List<Block>>> readBlockPlan(String filename) {
        InputStream inputStream = GameInformation.class.getResourceAsStream(filename);
        String blockPlan = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String[] steps = blockPlan.split("\n");
        List<Block> currentBlocks = new ArrayList<>();
        List<MutablePair<LocalDateTime, List<Block>>> hloPlans = new ArrayList<>();
        for (String step: steps) {
            if (step.contains("-starting")) {
                currentBlocks = new ArrayList<>();
            } else if (step.contains("!place-block")) {
                String[] stepArray = step.split(" ");
                int x = (int) Double.parseDouble(stepArray[2]);
                int y = (int) Double.parseDouble(stepArray[3]);
                int z = (int) Double.parseDouble(stepArray[4]);
                currentBlocks.add(new Block(x, y, z));
            } else if (step.contains("-finished")) {
                hloPlans.add(new MutablePair<>(null, currentBlocks));
            }
        }
        return hloPlans;
    }

    private List<MutablePair<LocalDateTime, List<Block>>> readHighlevelPlan(String filename) {
        InputStream inputStream = GameInformation.class.getResourceAsStream(filename);
        String blockPlan = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String[] steps = blockPlan.split("\n");
        List<Block> currentBlocks = new ArrayList<>();
        List<MutablePair<LocalDateTime, List<Block>>> hloPlans = new ArrayList<>();
        for (String step: steps) {
            if (step.contains("!build-")) {
                if (!currentBlocks.isEmpty()) {
                    hloPlans.add(new MutablePair<>(null, currentBlocks));
                }
                currentBlocks = new ArrayList<>();
            } else if (step.contains("!place-block-hidden")) {
                String[] stepArray = step.split(" ");
                int x = (int) Double.parseDouble(stepArray[2]);
                int y = (int) Double.parseDouble(stepArray[3]);
                int z = (int) Double.parseDouble(stepArray[4]);
                currentBlocks.add(new Block(x, y, z));
            }
        }
        hloPlans.add(new MutablePair<>(null, currentBlocks));
        return hloPlans;
    }

    private Set<Block> readInitialWorld(String filename) {
        Set<Block> worldBlocks = new HashSet<>();
        InputStream inputStream = GameInformation.class.getResourceAsStream(filename);
        String blockDescriptions = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String[] lines = blockDescriptions.split("\n");
        for (String line: lines) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] blockInfo = line.split(",");
            int x = Integer.parseInt(blockInfo[0]);
            int y = Integer.parseInt(blockInfo[1]);
            int z = Integer.parseInt(blockInfo[2]);
            worldBlocks.add(new Block(x, y, z));
        }
        return  worldBlocks;
    }

    /**
     * Writes the results of the evaluation methods above to a provided markdown file.
     */
    public void writeAnalysis(File file) throws IOException {
        FileWriter writer = new FileWriter(file);
        boolean wasSuccessful = wasSuccessful();
        String overview = "# Overview"
                + "\n - Connection from: "
                + getClientIp()
                + "\n - Player name: "
                + getPlayerName()
                + "\n - Scenario: "
                + getScenario()
                + "\n - Architect: "
                + getArchitect()
                + "\n - Successful: "
                + wasSuccessful
                + "\n\n## Times"
                + "\n - Start Time: "
                + getStartTime()
                + "\n - Success Time: "
                + (wasSuccessful ? getSuccessTime() : "not applicable")
                + "\n - End Time: "
                + getEndTime()
                + "\n - Experiment Duration: "
                + (wasSuccessful ? getTimeToSuccess() + " seconds" : "not applicable")
                + "\n - Total time logged in: "
                + getTotalTime()
                + " seconds\n\n## Blocks"
                + "\n - Number of blocks placed: "
                + getNumBlocksPlaced()
                + "\n - Number of blocks destroyed: "
                + getNumBlocksDestroyed()
                + "\n - Number of mistakes: "
                + getNumMistakes();
        writer.write(overview);
        writer.flush();

        List<Integer> blockDurations = getBlockPlacedDurations();
        StringBuilder durations = new StringBuilder("\n\n# Duration per block");
        for (int current: blockDurations) {
            durations.append("\n - ").append(current).append("ms");
        }

        if (wasSuccessful) {
            List<Pair<String, Integer>> HLODurations = getDurationPerHLO();
            durations.append("\n\n# Durations per High-level object");
            for (var pair : HLODurations) {
                durations.append("\n - ").append(pair.getFirst());
                durations.append(" : ").append(pair.getSecond());
                durations.append("ms");
            }

            List<Pair<String, Integer>> instructionDurations = getDurationPerInstruction();
            durations.append("\n\n# Durations per Instruction");
            for (var pair : instructionDurations) {
                durations.append("\n - ").append(pair.getFirst());
                durations.append(" : ").append(pair.getSecond());
                durations.append("ms");
            }
        }
        writer.write(durations.toString());
        writer.close();
    }

    /**
     * Prints three lists to the console.
     * - all blocks that were placed in this game until the given timestamp
     * - all blocks that were destroyed in this game until the given timestamp
     * - all blocks that were placed but not destroyed again until the given timestamp
     *
     */
    public void printBlocksUntilTimestamp(LocalDateTime time) {
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .and(GAME_LOGS.MESSAGE_TYPE.equal("BlockPlacedMessage")
                        .or(GAME_LOGS.MESSAGE_TYPE.equal("BlockDestroyedMessage")))
                .and(GAME_LOGS.TIMESTAMP.lessThan(time))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();

        List<Block> placedBlocks = new ArrayList<>();
        List<Block> destroyedBlocks = new ArrayList<>();
        List<Block> presentBlocks = new ArrayList<>();

        for (GameLogsRecord record: result) {
            Block curBlock = getBlockFromRecord(record);
            if (record.getMessageType().equals("BlockPlacedMessage")) {
                placedBlocks.add(curBlock);
                presentBlocks.add(curBlock);
            } else if (record.getMessageType().equals("BlockDestroyedMessage")) {
                destroyedBlocks.add(curBlock);
                presentBlocks.remove(curBlock);
            } else {
                throw new RuntimeException("Wrong message type " + record.getMessageType());
            }
        }

        System.out.println("Placed Blocks");
        for (Block block: placedBlocks) {
            System.out.println(" - " + block.toString());
        }

        System.out.println("Destroyed Blocks");
        for (Block block: destroyedBlocks) {
            System.out.println(" - " + block.toString());
        }

        System.out.println("Present Blocks");
        for (Block block: presentBlocks) {
            System.out.println(" - " + block.toString());
        }
    }

}
