package de.saar.minecraft.analysis;

import static de.saar.minecraft.broker.db.Tables.GAMES;
import static de.saar.minecraft.broker.db.Tables.GAME_LOGS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.saar.coli.minecraft.relationextractor.BigBlock;
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
import java.util.HashMap;
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

    static class Instruction {

        public LocalDateTime startTime;
        public String text;
        public Set<Block> blocks;
        public Integer duration;
        public int numWronglyAdded;
        public int numWronglyDestroyed;

        public Instruction(LocalDateTime startTime, String text, Set<Block> blocks) {
            this.startTime = startTime;
            this.text = text;
            this.blocks = blocks;
            this.numWronglyAdded = 0;
            this.numWronglyDestroyed = 0;
        }

        public void setDuration(int duration) {
            this.duration = duration;
        }

        public void setDuration(LocalDateTime endTime) {
            duration = (int) startTime.until(endTime, MILLIS);
        }

        public void addWronglyAdded() {
            numWronglyAdded++;
        }

        public void addWronglyDestroyed() {
            numWronglyDestroyed++;
        }

        public boolean isFinished() {
            return duration != null;
        }

        public int getNumMistakes(boolean withDestroyed) {
            if (withDestroyed) {
                return numWronglyAdded + numWronglyDestroyed;
            } else {
                return numWronglyAdded;
            }
        }

    }

    /**
     * Computes instruction durations and number of mistakes.
     *
     * Instruction durations begin with the text message that tells the player the instruction
     * and end if all blocks from this instruction are placed (and not removed before the
     * instruction was completed)
     */
    public List<Instruction> getInstructionDetails() {
        switch (getArchitect()) {
            case "SimpleArchitect-BLOCK": return getInstructionDetailsBlock();
            case "SimpleArchitect-MEDIUM": return getInstructionDetailsMedium();
            case "SimpleArchitect-HIGHLEVEL": return getInstructionDetailsHighlevel();
            default: return List.of();
        }
    }

    /**
     * Implementation of getInstructionDetails for SimpleArchitect-HIGHLEVEL
     * TODO: wrong duration if the block in question was set before the instruction. (Then the
     * next set block after the instruction is counted. --> Check for completeness for every
     * record not just BlockPlacedMessages?
     */
    private List<Instruction> getInstructionDetailsHighlevel() {
        if (getArchitect() == null || !getArchitect().equals("SimpleArchitect-HIGHLEVEL")) {
            throw new RuntimeException("Wrong architect type " + getArchitect());
        }
        List<Instruction> durations = new ArrayList<>();
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();

        Set<Block> blocksCurrentObjectLeft = null;
        List<Instruction> openInstructions = new ArrayList<>();
        Set<Block> presentBlocks = new HashSet<>();

        for (GameLogsRecord record : result) {
            // Get instruction beginnings
            if (record.getDirection().equals(GameLogsDirection.PassToClient)
                    && record.getMessageType().equals("TextMessage")) {
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (!jsonObject.has("text")) {
                    continue;
                }
                String newInstruction = jsonObject.get("text").getAsString();
                if (newInstruction.contains("Great! now")) {
                    if (blocksCurrentObjectLeft == null) {
                        logger.error("blocksCurrentObjectLeft is null at instruction "
                                + newInstruction);
                        continue;
                    }
                    var current = new Instruction(record.getTimestamp(), newInstruction,
                            blocksCurrentObjectLeft);
                    openInstructions.add(current);
                    blocksCurrentObjectLeft = null;
                } else if (newInstruction.contains("Please add this block again")) {
                    // Add a mistake to all openInstructions?
                    for (Instruction instruction: openInstructions) {
                        instruction.addWronglyDestroyed();
                    }
                } else if (newInstruction.contains("Not there! please remove that block again")) {
                    for (Instruction instruction: openInstructions) {
                        instruction.addWronglyAdded();
                    }
                }
            }
            // Get necessary blocks for next instruction
            if (record.getMessageType().equals("BlocksCurrentObjectLeft")) {
                if (record.getMessage().isEmpty()) {
                    continue;
                }
                String[] parts = record.getMessage().split(",\n");
                blocksCurrentObjectLeft = new HashSet<>();
                for (String part: parts) {
                    part = part.strip();
                    JsonObject jsonObject = JsonParser.parseString(part).getAsJsonObject();
                    if (jsonObject.get("type").getAsString().equals("Block")) {
                        int x = jsonObject.get("xpos").getAsInt();
                        int y = jsonObject.get("ypos").getAsInt();
                        int z = jsonObject.get("zpos").getAsInt();
                        blocksCurrentObjectLeft.add(new Block(x, y, z));
                    } else {
                        throw new NotImplementedException("Unknown type {}",
                                jsonObject.get("type").getAsString());
                    }
                }

            }
            // Get instruction ends
            if (record.getMessageType().equals("BlockPlacedMessage")) {
                // Update present blocks
                presentBlocks.add(getBlockFromRecord(record));
                // Check all open instructions for completeness and compute durations for completed
                for (Instruction instruction: openInstructions) {
                    if (presentBlocks.containsAll(instruction.blocks)) {
                        instruction.setDuration(record.getTimestamp());
                        durations.add(instruction);
                    }
                }
                // Remove completed instructions
                openInstructions.removeIf((x) -> presentBlocks.containsAll(x.blocks));
            }

            // Update present blocks
            if (record.getMessageType().equals("BlockDestroyedMessage")) {
                presentBlocks.remove(getBlockFromRecord(record));
            }
        }
        for (Instruction instruction: openInstructions) {
            logger.warn("Still open instruction");
            logger.warn(instruction.text);
            logger.warn(instruction.startTime);
            logger.warn(instruction.blocks);
        }
        return durations;
    }

    /**
     * Implementation of getInstructionDetails for SimpleArchitect-MEDIUM
     */
    private List<Instruction> getInstructionDetailsMedium() {
        if (getArchitect() == null || !getArchitect().equals("SimpleArchitect-MEDIUM")) {
            throw new RuntimeException("Wrong architect type " + getArchitect());
        }
        List<Instruction> durations = new ArrayList<>();
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();

        List<Instruction> openInstructions = new ArrayList<>();
        Set<Block> presentBlocks;

        if (getScenario().equals("house")) {
            presentBlocks = readInitialWorld("/de/saar/minecraft/worlds/house.csv");
        } else if (getScenario().equals("bridge")) {
            presentBlocks = readInitialWorld("/de/saar/minecraft/worlds/bridge.csv");
        } else {
            throw new NotImplementedException("Unknown scenario: " + getScenario());
        }

        // TODO: can there be several current objects of the same type?
        HashMap<String, Set<Block>> currentObjects = new HashMap<>();

        for (GameLogsRecord record : result) {
            if (record.getDirection().equals(GameLogsDirection.PassToClient)
                    && record.getMessageType().equals("TextMessage")) {
                // Text messages that can mark an instruction beginning
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (!jsonObject.has("text")) {
                    continue;
                }
                String newInstruction = jsonObject.get("text").getAsString();
                if (newInstruction.contains("Now I will teach you")) {
                    // get Introduction Message from current Objects
                    Set<Block> currentBlocks = currentObjects.remove("IntroductionMessage");
                    openInstructions.add(new Instruction(record.getTimestamp(), newInstruction,
                            currentBlocks));
                } else if (newInstruction.contains("put a block")) {
                    // Check if there is still a currentObject, there are many "put a block"
                    // instructions that are just repetitions from earlier instructions
                    if (currentObjects.containsKey("Block")) {
                        Set<Block> currentBlocks = currentObjects.remove("Block");
                        openInstructions.add(new Instruction(record.getTimestamp(),
                                newInstruction, currentBlocks));
                    }
                } else if (newInstruction.contains("build a")) {
                    // Instructions for an HLO that was introduced earlier
                    logger.info("build instruction " + newInstruction);
                    if (currentObjects.containsKey("FollowUp")) {
                        Set<Block> currentBlocks = currentObjects.remove("FollowUp");
                        openInstructions.add(new Instruction(record.getTimestamp(),
                                newInstruction, currentBlocks));
                    }
                } else if (newInstruction.contains("Congratulations")) {
                    // End remaining open instructions
                    // Ideally, the architect should only congratulate if all instructions were
                    // completed.
                    if (! openInstructions.isEmpty()) {
                        logger.info("present blocks {}", presentBlocks);
                        logger.error("Still open instructions after completion.");
                        for (Instruction instruction: openInstructions) {
                            logger.info(instruction.text);
                            logger.info(instruction.blocks);
                            durations.add(instruction);
                        }
                    }
                } else if (newInstruction.contains("Please add this block again")) {
                    for (Instruction instruction: openInstructions) {
                        instruction.addWronglyDestroyed();
                    }
                } else if (newInstruction.contains("Not there! please remove that block again")) {
                    for (Instruction instruction : openInstructions) {
                        instruction.addWronglyAdded();
                    }
                }
            } else if (record.getMessageType().equals("CurrentObject")) {
                // Get necessary blocks for next instruction
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                String type = jsonObject.get("type").getAsString();
                switch (type) {
                    case "Block":
                        Block current = getBlockFromJson(jsonObject);
                        currentObjects.put(type, Set.of(current));
                        break;
                    case "IntroductionMessage":
                        String name = jsonObject.get("name").getAsString();
                        JsonObject object = jsonObject.get("object").getAsJsonObject();
                        currentObjects.put(type, getBlocksFromObject(object, name));
                        break;
                    case "Wall":
                    case "Row":
                    case "Railing":
                        Set<Block> currentBlocks = getBlocksFromObject(jsonObject, type.toLowerCase());
                        currentObjects.put("FollowUp", currentBlocks);
                        break;
                    default:
                        logger.error("Unknown current object {}", type);
                        break;
                }
            } else if (record.getMessageType().equals("BlockPlacedMessage")) {
                presentBlocks.add(getBlockFromRecord(record));
                // Check all open instructions for completeness and compute durations for completed
                for (Instruction instruction: openInstructions) {
                    if (presentBlocks.containsAll(instruction.blocks)) {
                        instruction.setDuration(record.getTimestamp());
                        durations.add(instruction);
                    }
                }
                // Remove completed instructions
                openInstructions.removeIf((x) -> presentBlocks.containsAll(x.blocks));
            } else if (record.getMessageType().equals("BlockDestroyedMessage")) {
                presentBlocks.remove(getBlockFromRecord(record));
            }
        }

        for (Instruction instruction: openInstructions) {
            logger.warn("Still open instruction");
            logger.warn(instruction.text);
            logger.warn(instruction.startTime);
            logger.warn(instruction.blocks);
        }
        return durations;
    }

    /**
     * Implementation of getInstructionDetails for SimpleArchitect-BLOCK.
     *
     * Assumption: "Please add this block again" is not an instruction
     */
    private List<Instruction> getInstructionDetailsBlock() {
        if (getArchitect() == null || !getArchitect().equals("SimpleArchitect-BLOCK")) {
            return List.of();
        }
        List<Instruction> durations = new ArrayList<>();
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();
        Block currentObjectLeft = null;
        List<Instruction> currentInstructions = new ArrayList<>();
        Set<Block> allInstructedBlocks = new HashSet<>();

        for (GameLogsRecord record: result) {
            if (record.getMessageType().equals("BlocksCurrentObjectLeft")) {
                if (currentObjectLeft != null) {
                    logger.info(currentObjectLeft);
                    logger.error("Object changed too early {}", record.getTimestamp());
                    continue;
                }
                if (record.getMessage().isEmpty()) {
                    logger.info("Empty BlockCurrentObjectLeft message {}", record.getTimestamp());
                    continue;
                }
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                int x = jsonObject.get("xpos").getAsInt();
                int y = jsonObject.get("ypos").getAsInt();
                int z = jsonObject.get("zpos").getAsInt();
                currentObjectLeft = new Block(x, y, z);
            } else if (record.getMessageType().equals("TextMessage")
                    && record.getDirection().equals(GameLogsDirection.PassToClient)) {
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (! jsonObject.has("text")) {
                    continue;
                }
                String newInstruction = jsonObject.get("text").getAsString();
                if (currentObjectLeft != null) {
                    // Start new instruction
                    currentInstructions.add(new Instruction(record.getTimestamp(),
                            newInstruction, Set.of(currentObjectLeft)));
                    allInstructedBlocks.add(currentObjectLeft);
                    currentObjectLeft = null;
                }
            } else if (record.getMessageType().equals("BlockPlacedMessage")) {
                // Check instruction and object
                Block newBlock = getBlockFromRecord(record);
                for (Instruction instruction: currentInstructions) {
                    if (instruction.blocks.contains(newBlock)) {
                        instruction.setDuration(record.getTimestamp());
                        durations.add(instruction);
                    } else {
                        instruction.addWronglyAdded();
                    }
                }
                currentInstructions.removeIf(Instruction::isFinished);
            } else if (record.getMessageType().equals("BlockDestroyedMessage")) {
                // Block is part of this instruction or part of a completed earlier instruction
                if (allInstructedBlocks.contains(getBlockFromRecord(record))) {
                    for (Instruction instruction: currentInstructions) {
                        instruction.addWronglyDestroyed();
                    }
                }
            }
        }
        for (Instruction instruction: currentInstructions) {
            logger.warn("Still open instruction");
            logger.warn(instruction.text);
            logger.warn(instruction.startTime);
            logger.warn(instruction.blocks);
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
     * Reads a json object from a CurrentObject message and returns a set of blocks that are part
     * of that object
     * @param object: a JsonObject x from the parent: {"object": x}
     * @param name: the name of the object, from the parent {"name": x}
     */
    private Set<Block> getBlocksFromObject(JsonObject object, String name) {
        if (name.equals("floor") || name.equals("wall") || name.equals("row")) {
            BigBlock bigBlock = getBigBlockFromJson(object);
            return bigBlock.getBlocks();
        }
        if (name.equals("railing")) {
            Block b1 = getBlockFromJson(object.get("block1").getAsJsonObject());
            Block b2 = getBlockFromJson(object.get("block2").getAsJsonObject());
            BigBlock row = getBigBlockFromJson(object.get("row").getAsJsonObject());
            Set<Block> blocks = row.getBlocks();
            blocks.add(b1);
            blocks.add(b2);
            return blocks;
        }
        throw new NotImplementedException("Unknown object " + name);
    }

    /**
     * @param object: e.g. from "block1":{"xpos":6,"ypos":67,"zpos":6,"children":[]}
     */
    private Block getBlockFromJson(JsonObject object) {
        int x = object.get("xpos").getAsInt();
        int y = object.get("ypos").getAsInt();
        int z = object.get("zpos").getAsInt();
        return new Block(x, y, z);
    }

    /**
     * @param object, e.g. from: "row":{"x1":6,"y1":68,"z1":6,"x2":10,"y2":68,"z2":6,
     *                "name":"row-railing", ...
     */
    private BigBlock getBigBlockFromJson(JsonObject object) {
        String name = object.get("name").getAsString();
        int x1 = object.get("x1").getAsInt();
        int y1 = object.get("y1").getAsInt();
        int z1 = object.get("z1").getAsInt();
        int x2 = object.get("x2").getAsInt();
        int y2 = object.get("y2").getAsInt();
        int z2 = object.get("z2").getAsInt();

        return new BigBlock(name, x1, y1, z1, x2, y2, z2);
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
                        if (record.getMessage().contains("Great!")) {
                            firstInstructionTime = record.getTimestamp();
                        }
                    } else if (record.getMessage().contains("Congratulations, you are done building a")) {
                        if (hloPlans.get(hloPlans.size() - 1).getLeft() == null) {
                            hloPlans.get(hloPlans.size() - 1).setLeft(record.getTimestamp());
                        }
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
            // TODO: is the block type important? Not all of the initial blocks are unique blocks
            String typeName = blockInfo[3];
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

            List<Instruction> details = getInstructionDetails();
            durations.append("\n\n# Durations per Instruction");
            for (var instruction : details) {
                durations.append("\n - ").append(instruction.text);
                durations.append(" : ").append(instruction.duration);
                durations.append("ms, ");
                durations.append(instruction.getNumMistakes(countDestroyedAsMistake));
                durations.append(" mistakes (wrongly added: ");
                durations.append(instruction.numWronglyAdded);
                durations.append(", wrongly destroyed: ");
                durations.append(instruction.numWronglyDestroyed);
                durations.append(")");
            }
            writer.write(durations.toString());
        }
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
