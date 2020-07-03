package de.saar.minecraft.analysis;

import static de.saar.minecraft.broker.db.Tables.GAMES;
import static de.saar.minecraft.broker.db.Tables.GAME_LOGS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.saar.coli.minecraft.relationextractor.Block;
import de.saar.minecraft.broker.db.GameLogsDirection;
import de.saar.minecraft.broker.db.Tables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import de.saar.minecraft.broker.db.tables.records.GameLogsRecord;
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
     * Returns gameid Scenario Architect wassucessful timetosuccess numblocksplaced numblocksdestroyed nummistakes answers
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

    public List<Pair<String, Integer>> getDurationPerBlockInstruction2() {
        if (getArchitect() == null || !getArchitect().equals("SimpleArchitect-BLOCK")) {
            return List.of();
        }
        List<Pair<String, Integer>> durations = new ArrayList<>();
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();
        String oldInstruction = null;
        LocalDateTime oldTimestamp = null;
        Block currentObjectLeft = null;

        for(GameLogsRecord record: result) {
            if (record.getMessageType().equals("BlocksCurrentObjectLeft")) {
                if (currentObjectLeft != null) {
                    logger.error("Object changed too early");
                }
                if (record.getMessage().isEmpty()) {
                    continue;
                }

                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                int x = jsonObject.get("xpos").getAsInt();
                int y = jsonObject.get("ypos").getAsInt();
                int z = jsonObject.get("zpos").getAsInt();
                String type = jsonObject.get("type").getAsString();
                currentObjectLeft = new Block(x, y, z);
                oldTimestamp = record.getTimestamp();
            } else if (record.getMessageType().equals("TextMessage")
                    && record.getDirection().equals(GameLogsDirection.PassToClient)) {
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
                    } else {
                        logger.error("Instruction changed too early");
                    }
                }

            } else if (record.getMessageType().equals("BlockPlacedMessage")) {
                // Check instruction and object
                JsonObject jsonObject = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                // TODO: deal with incomplete BlockPlacedMessages
                if (!jsonObject.has("x") || !jsonObject.has("y") || !jsonObject.has("z")) {
                    continue;
                }
                int x = jsonObject.get("x").getAsInt();
                int y = jsonObject.get("y").getAsInt();
                int z = jsonObject.get("z").getAsInt();

                Block newBlock = new Block(x, y, z);
                if (currentObjectLeft == null) {
                    // no block should be placed
                    continue;
                }
                if (currentObjectLeft.equals(newBlock)) {
                    // correct block
                    int duration = (int)oldTimestamp.until(record.getTimestamp(), MILLIS);
                    durations.add(new Pair<>(oldInstruction, duration));
                    oldInstruction = null;
                    oldTimestamp = null;
                    currentObjectLeft = null;
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
     *
     * The method assumes that all HLOs were built in the correct order. The duration for each HLO starts when the
     * previous HLO is finished (or for the first with the welcome message).
     */
    public List<Pair<String, Integer>> getDurationPerHLOCurrentWorldBased() {
        assert wasSuccessful();
        if (getArchitect() == null) {
            return List.of();
        }
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();

        LocalDateTime firstInstructionTime = null;
        List<MutablePair<LocalDateTime, List<Block>>> hloPlans;
        if (getScenario().equals("bridge")) {
            var floorBlocks = List.of(
                    new Block(7, 66, 6),
                    new Block(8, 66, 6),
                    new Block(9, 66, 6),
                    new Block(10, 66, 6),
                    new Block(10, 66, 7),
                    new Block(9, 66, 7),
                    new Block(8, 66, 7),
                    new Block(7, 66, 7),
                    new Block(6, 66, 7),
                    new Block(6, 66, 8),
                    new Block(7, 66, 8),
                    new Block(8, 66, 8),
                    new Block(9, 66, 8)
            );
            var firstRailingBlocks = List.of(
                    new Block(10, 67, 6),
                    new Block(6, 67, 6),
                    new Block(6, 68, 6),
                    new Block(7, 68, 6),
                    new Block(8, 68, 6),
                    new Block(9, 68, 6),
                    new Block(10, 68, 6)
            );
            var secondRailingBlocks = List.of(
                    new Block(6, 67, 8),
                    new Block(10, 67, 8),
                    new Block(10, 68, 8),
                    new Block(9, 68, 8),
                    new Block(8, 68, 8),
                    new Block(7, 68, 8),
                    new Block(6, 68, 8)
            );

            hloPlans = List.of(
                    new MutablePair<>(null, floorBlocks),
                    new MutablePair<>(null, firstRailingBlocks),
                    new MutablePair<>(null, secondRailingBlocks)
            );
        } else if (getScenario().equals("house")) {
            String scenario = getScenario();
            // TODO read block plan from
            // '/shared-resources/src/main/resources/de/saar/minecraft/domains/house-block.plan'

            InputStream inputStream = GameInformation.class.getResourceAsStream("/de/saar/minecraft/domains/" + scenario + "-highlevel.plan");
            String blockPlan = new BufferedReader(new InputStreamReader(inputStream))
                    .lines()
                    .collect(Collectors.joining("\n"));
            String[] steps = blockPlan.split("\n");
            List<Block> currentBlocks = new ArrayList<>();
            hloPlans = new ArrayList<>();
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
//                } else if (step.contains("-finished")) {
//                    hloPlans.add(new MutablePair<>(null, currentBlocks));
                }

            }
            hloPlans.add(new MutablePair<>(null, currentBlocks));

        } else {
            throw new NotImplementedException("Unknown scenario: " + getScenario());
        }
        for (GameLogsRecord record: result) {
            if (record.getMessageType().equals("CurrentWorld")) {
                JsonArray jsonArray = JsonParser.parseString(record.getMessage()).getAsJsonArray();
                Gson gson = new Gson();
                Set<Block> world = null;
                switch (getArchitect()) {
                    case "SimpleArchitect-BLOCK": {
                        Block[] blockArray = gson.fromJson(jsonArray, Block[].class);
                        world = Set.of(blockArray);
                        break;
                    }
                    case "SimpleArchitect-MEDIUM":
                    case "SimpleArchitect-HIGHLEVEL": {
                        world = new HashSet<>();
                        for (JsonElement el: jsonArray) {
                            if (el.isJsonObject()) {
                                JsonObject curObject = el.getAsJsonObject();
                                String type = curObject.get("type").getAsString();
                                switch (type) {
                                    case "Block": {
                                        world.add(gson.fromJson(curObject, Block.class));
                                        break;
                                    }
                                    case "Floor":
                                    case "Railing":
                                    case "Wall":
                                    case "Row": {
                                        addBlocksFromChildren(world, curObject);
                                        break;
                                    }
                                    case "UniqueBlock":
                                        // is not placed by the player
                                        // but is present in highlevel plan
                                        world.add(gson.fromJson(curObject, Block.class));
                                        continue;
                                    default:
                                        throw new NotImplementedException("Unknown Minecraft object type: " + type);
                                }
                            }
                        }
                        break;
                    }
                    case "DummyArchitect": {
                        // no HLO instructions
                        return List.of();
                    }
                    default:
                        throw new NotImplementedException("Unknown Architect: " + getArchitect());
                }
                for (MutablePair<LocalDateTime, List<Block>> hlo: hloPlans) {
                    if (hlo.getLeft() == null) {
                        boolean allPresent = true;
                        for (Block block: hlo.getRight()) {
                            if (!world.contains(block)) {
                                allPresent = false;
                                break;
                            }
                        }
                        if (allPresent) {
                            hlo.setLeft(record.getTimestamp());
                        }
                    }
                }

            } else if (record.getMessageType().equals("TextMessage") ) {
                if (record.getMessage().contains("Welcome! I will try to instruct you to build a ")) {
                    firstInstructionTime = record.getTimestamp();
                } else if (record.getMessage().contains("Congratulations, you are done building a")) {
                    if (hloPlans.get(hloPlans.size() - 1).getLeft() == null) {
                        hloPlans.get(hloPlans.size() - 1).setLeft(record.getTimestamp());
                    }
                }
            }
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

    private void addBlocksFromChildren(Set<Block> world, JsonObject jsonObject) {
        Gson gson = new Gson();
        for (var childElement: jsonObject.get("children").getAsJsonArray()){
            if (childElement.isJsonObject()) {
                JsonObject child = (JsonObject)childElement;
                if ( child.has("xPos")) {
                    world.add(gson.fromJson(child, Block.class));
                } else if (child.has("name")) {
                    addBlocksFromChildren(world, child);
                }
            }
        }
    }

    public List<Pair<String, Integer>> getDurationPerHLO() {
        assert wasSuccessful();
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

        // Add initial blocks
        for (GameLogsRecord record: result) {
            if (record.getMessageType().equals("BlockPlacedMessage")) {
                JsonObject json = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (json.has("x") && json.has("y") && json.has("z")) {
                    presentBlocks.add(new Block(json.get("x").getAsInt(), json.get("y").getAsInt(), json.get("z").getAsInt()));
                } else {
                    logger.error("Block log at {} is not complete: {}", record.getTimestamp(), json);
                }
            } else if (record.getMessageType().equals("BlockDestroyedMessage")) {
                JsonObject json = JsonParser.parseString(record.getMessage()).getAsJsonObject();
                if (json.has("x") && json.has("y") && json.has("z")) {
                    presentBlocks.remove(new Block(json.get("x").getAsInt(), json.get("y").getAsInt(), json.get("z").getAsInt()));
                } else {
                    logger.error("Block log at {} is not complete: {}", record.getTimestamp(), json);
                }
            } else if (record.getMessageType().equals("TextMessage")) {
                if (firstInstructionTime == null) {
                    if (record.getMessage().contains("Great!")) {
                        firstInstructionTime = record.getTimestamp();
                    }
                } else if (record.getMessage().contains("Congratulations, you are done building a")) {
                    if (hloPlans.get(hloPlans.size() - 1).getLeft() == null) {
                        hloPlans.get(hloPlans.size() - 1).setLeft(record.getTimestamp());
                    }
                }
            }
            for (MutablePair<LocalDateTime, List<Block>> hlo : hloPlans) {
                if (hlo.getLeft() == null) {
                    boolean allPresent = true;
                    for (Block block : hlo.getRight()) {
                        if (!presentBlocks.contains(block)) {
                            allPresent = false;
                            break;
                        }
                    }
                    if (allPresent) {
                        hlo.setLeft(record.getTimestamp());
                    }
                }
            }
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
        Set<Block> worldBlocks = new HashSet<Block>();
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
            worldBlocks.add(new Block(x,y,z));
        }
        return  worldBlocks;
    }

    /**
     * Writes the results of the evaluation methods above to a provided markdown file.
     */
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
                " seconds\n\n## Blocks\n" +
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

        if (wasSuccessful) {

            List<Pair<String, Integer>> HLODurations = getDurationPerHLO();
            durations.append("\n\n# Durations per High-level object");
            for (var pair : HLODurations) {
                durations.append("\n - ").append(pair.getFirst());
                durations.append(" : ").append(pair.getSecond());
                durations.append("ms");
            }

            List<Pair<String, Integer>> HLODurations2 = getDurationPerHLOCurrentWorldBased();
            durations.append("\n\n# Durations per High-level object (current world based)");
            for (var pair : HLODurations2) {
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

            instructionDurations = getDurationPerBlockInstruction2();
            durations.append("\n\n# Durations per Block Instruction");
            for (var pair : instructionDurations) {
                durations.append("\n - ").append(pair.getFirst());
                durations.append(" : ").append(pair.getSecond());
                durations.append("ms");
            }

            writer.write(durations.toString());
        }
        writer.close();
    }

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
            JsonObject json = JsonParser.parseString(record.getMessage()).getAsJsonObject();
            Block curBlock = new Block(json.get("x").getAsInt(), json.get("y").getAsInt(), json.get("z").getAsInt());
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
