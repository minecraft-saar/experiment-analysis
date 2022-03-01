package de.saar.minecraft.analysis;

import static de.saar.minecraft.broker.db.Tables.GAMES;
import static de.saar.minecraft.broker.db.Tables.GAME_LOGS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.common.collect.HashMultiset;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.saar.coli.minecraft.relationextractor.Block;
import de.saar.minecraft.broker.db.Tables;
import de.saar.minecraft.broker.db.tables.records.GameLogsRecord;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;


/**
 * This class extracts information from a single game, identified by the game ID.  Most
 * methods should be self-explanatory, but there is one thing you might ask when inspecting the code:
 * Why use multisets to keep track of what is being built in getDurationPerHLO??
 *
 * It was first implemented with sets, but then we found a game where destroy and place were in
 * the wrong order: place, place, destroy (which is impossible for the same location).
 * It seems like the second place and destroy happened at the same tick and were reported
 * in the wrong order for some reason.  Using a multiset means we now have one block there
 * (1+1-1=1) instead of zero (the second placement was ignored with a set as that block already
 * existed in the world).  So this is a workaround for a very rare event ordering bug somewhere else.
 */
public class GameInformation {
    int gameId;
    DSLContext jooq;
    boolean countDestroyedAsMistake;
    List<Pair<String, HLOInformation>> hloInformation = null;
    List<Pair<String, Integer>> instructionDurations = null;

    /**
     * The ID of the log message showing that the user was successful.
     */
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

    /**
     * for format details see saveCSV(File file) in AggregateInformation.java
     */
    public String getCSVHeader(String separator, int maxInstructionDurationsSize) {
        var sb = new StringBuilder();
        int qnum = 0;
        for (var qa : getNumericQuestions()
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

        if (getScenario().equals("bridge")) {
            for (int i = 0; i < 3; i++) {
                sb.append(separator);
                sb.append("HLO").append(i);
            }
            for (int i = 0; i < 3; i++) {
                sb.append(separator);
                sb.append("HLOmistakes").append(i);
            }
        } else {
            //maybe has to be adapted in the future if number of HighLevelObjects bigger than 8
            for (int i = 0; i < 8; i++) {
                sb.append(separator);
                sb.append("HLO").append(i);
            }
            for (int i = 0; i < 8; i++) {
                sb.append(separator);
                sb.append("HLOmistakes").append(i);
            }
        }

        for (int i = 0; i < maxInstructionDurationsSize; i++) {
            sb.append(separator);
            sb.append("Instruction").append(i);
            sb.append(separator);
            sb.append("Time");
        }

        for (int i = 0; i < getNumericQuestions().size(); i++) {
            sb.append(separator).append("Question").append(i);
        }
        return sb.append("\n").toString();
    }

    /**
     * for format details see saveCSV(File file) in AggregateInformation.java
     *
     * @param separator the separator of the fields
     */
    public String getCSVLine(String separator, int maxInstructionDurationsSize) {
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
            List<Pair<String, HLOInformation>> hloTimings;
            if (this.hloInformation == null) {
                hloTimings = getHLOInformation();
                this.hloInformation = hloTimings;
            } else {
                hloTimings = this.hloInformation;
            }
            if (getScenario().equals("bridge")) {
                for (int i = 0; i < 3; i++) {
                    sb.append(separator);
                    if (i < hloTimings.size()) {
                        sb.append(hloTimings.get(i).getSecond().duration);
                    } else {
                        sb.append("NA");
                    }
                }
                for (int i = 0; i < 3; i++) {
                    sb.append(separator);
                    if (i < hloTimings.size()) {
                        sb.append(hloTimings.get(i).getSecond().mistakes);
                    } else {
                        sb.append("NA");
                    }
                }
            } else {
                //maybe has to be adapted in the future if number of HighLevelObjects bigger than 8
                for (int i = 0; i < 8; i++) {
                    sb.append(separator);
                    if (i < hloTimings.size()) {
                        sb.append(hloTimings.get(i).getSecond().duration);
                    } else {
                        sb.append("NA");
                    }
                }
                for (int i = 0; i < 8; i++) {
                    sb.append(separator);
                    if (i < hloTimings.size()) {
                        sb.append(hloTimings.get(i).getSecond().mistakes);
                    } else {
                        sb.append("NA");
                    }
                }
            }

            for (int i = 0; i < maxInstructionDurationsSize; i++) {
                sb.append(separator);
                if ((instructionDurations != null) && (i < instructionDurations.size())) {
                    Pair<String, Integer> entry = instructionDurations.get(i);
                    String instruction = entry.getFirst();
                    String[] instructionList = instruction.split(",");
                    String ins = instructionList[1];
                    sb.append(ins.substring(27, ins.length() - 2));
                    sb.append(separator);
                    sb.append(entry.getSecond());
                } else {
                    sb.append("NA");
                    sb.append(separator);
                    sb.append("NA");
                }
            }
        } else {
            sb.append((separator + "NA").repeat(16));
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

    enum InstructionLevel {
        BLOCK, TEACHING, HIGHLEVEL
    }

    ;

    public InstructionLevel inferInstructionLevel() {
        var countTeach = jooq.selectCount()
                .from(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.eq(this.gameId))
                .and(GAME_LOGS.MESSAGE.contains("teach you"))
                .fetchOne().component1();
        if (countTeach > 0) {
            return InstructionLevel.TEACHING;
        }
        var countHLO = jooq.selectCount()
                .from(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.eq(this.gameId))
                .and(GAME_LOGS.MESSAGE.contains("a wall")
                        .or(GAME_LOGS.MESSAGE.contains("a floor")))
                .fetchOne().component1();
        if (countHLO > 0) {
            return InstructionLevel.HIGHLEVEL;
        }
        return InstructionLevel.BLOCK;
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
     * @return number of times the architect messages about an incorrectly placed block
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
     * @return number of mistakes in a given timespan
     */
    public int getNumMistakesInTimespan(LocalDateTime begin, LocalDateTime end) {

        Result<Record1<LocalDateTime>> result = jooq.select(GAME_LOGS.TIMESTAMP)
                .from(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.eq(gameId))
                .and(GAME_LOGS.ID.lessOrEqual(successMessageID))
                .and(GAME_LOGS.MESSAGE.contains("Not there! please remove that block again"))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();
        int count = 0;
        if (result.isNotEmpty()) {
            for (int i = 0; i < result.size(); i++) {
                LocalDateTime current = result.getValue(i, GAME_LOGS.TIMESTAMP);
                if (current.isAfter(begin) && current.isBefore(end)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * @return list of question-answer pairs where the answer is a number
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
     * @return list of question-answer pairs where the answer is not a number
     */
    public List<Pair<String, String>> getFreeformQuestions() {
        return jooq.selectFrom(Tables.QUESTIONNAIRES)
                .where(Tables.QUESTIONNAIRES.GAMEID.equal(gameId))
                .orderBy(Tables.QUESTIONNAIRES.ID.asc())
                .fetchStream()
                .filter((row) -> !NumberUtils.isDigits(row.getAnswer()))
                .map((row) -> new Pair<>(row.getQuestion(), row.getAnswer()))
                .collect(Collectors.toList());
    }

    /**
     * @return true if the game was successfully finished, false if stopped early
     */
    public boolean wasSuccessful() {
        return successMessageID < Long.MAX_VALUE;
    }

    /**
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
     * returns the duration until the user logged out.
     * Note: This may be much longer than until task completion!
     *
     * @return Seconds elapsed between login and logout
     */
    public long getTotalTime() {
        var timeStarted = getStartTime();
        var timeFinished = getEndTime();
        return timeStarted.until(timeFinished, SECONDS);
    }

    /**
     * @return the first timestamp of the game
     */
    public LocalDateTime getStartTime() {
        return jooq.select(GAME_LOGS.TIMESTAMP)
                .from(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.eq(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetchAny(GAME_LOGS.TIMESTAMP);
    }

    /**
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
     * Currently only tested for bridge scenarios,
     * should also be applicable for house scenarios, but not tested yet
     */
    public void getDurationPerInstruction() {
        instructionDurations = new ArrayList<>();
        var query = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.ID.asc())
                .fetch();
        String oldInstruction = null;
        LocalDateTime oldTimestamp = null;
        int skip = 0;
        int wrongBlocks = 0;
        int greatMessages = 0;
        int duration = 0;
        boolean getTimeAndInstruction = false;
        for (GameLogsRecord record : query) {
            if (record.getMessageType().equals("TextMessage")) {
                String instruction = record.getMessage();
                if (getTimeAndInstruction) {
                    getTimeAndInstruction = false;
                    oldTimestamp = record.getTimestamp();
                    oldInstruction = instruction;
                }
                if (instruction.contains("Welcome!")
                        || instruction.contains("spacebar") || instruction.contains("correct")) {
                    skip = 0;
                    getTimeAndInstruction = true;
                    continue;
                } else {
                    if (instruction.contains("Not there!")) {
                        skip++;
                        wrongBlocks++;
                    } else if (instruction.contains("Great!")) {
                        if (skip > 0) {
                            greatMessages++;
                            skip--;
                        }
                        if (instruction.contains("new") && instruction.contains("true")) {
                            duration = (int) oldTimestamp.until(record.getTimestamp(), MILLIS);
                            instructionDurations.add(new Pair(oldTimestamp + ": " + oldInstruction, duration));
                            if (instruction.contains("teach") || instruction.contains("finished building")) {
                                getTimeAndInstruction = true;
                                continue;
                            }
                            oldTimestamp = record.getTimestamp();
                            oldInstruction = instruction;
                        }
                    } else if (instruction.contains("Congratulations")) {
                        duration = (int) oldTimestamp.until(record.getTimestamp(), MILLIS);
                        instructionDurations.add(new Pair(oldTimestamp + ": " + oldInstruction, duration));
                        break;
                    }
                }
            }
        }
        if (greatMessages != wrongBlocks) {
            System.out.println("Some wrong blocks weren't deleted");
        }
    }

    /**
     * reads a BlockMessage and returns a Block with the same coordinates
     *
     * @param record a GamesLogsRecord for a BlockPlaced- or BlockDestroyedMessage
     */
    private Block getBlockFromRecord(GameLogsRecord record) {
        JsonObject json = JsonParser.parseString(record.getMessage()).getAsJsonObject();
        // If Block logs are incomplete, the missing values are the default 0
        // This should only occur for games that were played before
        // TODO: date of setting up infrastructure commit 96b2fde on the server
        int x = 0;
        int y = 0;
        int z = 0;
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

    public static class HLOGatherer {
        public List<Block> blocks;
        public LocalDateTime timestamp;
        public int mistakes;

        public HLOGatherer(List<Block> blocks) {
            this.blocks = blocks;
        }
    }

    public static class HLOInformation {
        public final int duration;
        public final int mistakes;

        public HLOInformation(int duration, int mistakes) {
            this.duration = duration;
            this.mistakes = mistakes;
        }
    }

    /**
     * Returns the time the user needed to build each HLO in the scenario, i.e. the walls
     * in the house scenario and the floor and railings in the bridge scenario.
     * e.g. [("wall", 8000), ("wall", 5000), ...] or [("floor", 3000), ... ("railing", 4000)]
     * Regardless of whether it was instructed per block or as a HLO.
     * <p>
     * If the game was not successful, an empty list is returned.
     *
     * <p>The method assumes that all HLOs were built in the correct order. The duration for each
     * HLO starts when the previous HLO is finished (or for the first with the welcome message).</p>
     */
    public List<Pair<String, HLOInformation>> getHLOInformation() {
        instructionDurations = new ArrayList<>();
        if (!wasSuccessful()) {
            return List.of();
        }
        if (getArchitect() == null) {
            return List.of();
        }
        Result<GameLogsRecord> result = jooq.selectFrom(GAME_LOGS)
                .where(GAME_LOGS.GAMEID.equal(gameId))
                .orderBy(GAME_LOGS.TIMESTAMP.asc())
                .fetch();

        List<HLOGatherer> hloPlans;
        HashMultiset<Block> presentBlocks = HashMultiset.create();
        HashMap<Block, LocalDateTime> blocksTime = new HashMap<>();
        String scenario = getScenario();
        if (scenario.equals("house")) {
            hloPlans = readHighlevelPlan("/de/saar/minecraft/domains/house-highlevel.plan")
                    .stream()
                    .map(HLOGatherer::new)
                    .collect(Collectors.toList());
            presentBlocks.addAll(readInitialWorld("/de/saar/minecraft/worlds/house.csv"));
        } else if (scenario.equals("bridge")) {
            hloPlans = readBlockPlan("/de/saar/minecraft/domains/bridge-block.plan").stream()
                    .map(HLOGatherer::new)
                    .collect(Collectors.toList());
            presentBlocks.addAll(readInitialWorld("/de/saar/minecraft/worlds/bridge.csv"));
        } else {
            throw new NotImplementedException("Scenario {} is not implemented", scenario);
        }

        LocalDateTime firstInstructionTime = null;
        int numMistakes = 0;

        /* These two booleans implement a work-around for https://github.com/minecraft-saar/infrastructure/issues/19
        Sometimes, a delete message and a create message are in the wrong order.  This workaround is for a specific
        game where the player did a put-delete-put sequence for one location.  The result should be that there is a
        block, but for some reason the sequence ended up as put-put-delete, meaning the world as we capture it here
        does not have the block even though it was there during the game.  If that happens, we disable tracking
        of deletions in that one game.
        */
        boolean dataExtracted = false;
        boolean ignoreDestroyMessages = false;
        LocalDateTime lastTimestamp = null;
        int count = 1;
        while (!dataExtracted) {
            // go through the complete game and keep track of when instructions where given,
            // blocks placed an HLO completed.
            for (GameLogsRecord record : result) {
                switch (record.getMessageType()) {
                    case "BlockPlacedMessage":
                        Block block = getBlockFromRecord(record);
                        presentBlocks.add(block);
                        if (blocksTime.containsKey(block)) {
                            blocksTime.replace(block, record.getTimestamp());
                        } else {
                            blocksTime.put(block, record.getTimestamp());
                        }
                        break;
                    case "BlockDestroyedMessage":
                        if (!ignoreDestroyMessages) {
                            presentBlocks.remove(getBlockFromRecord(record));
                        }
                        break;
                    case "TextMessage":
                        if (firstInstructionTime == null) {
                            // the first *instruction* has a derivation tree, otherwise it is
                            // a welcome message or similar.
                            if (record.getMessage().contains("\\\"tree\\\":")) {
                                firstInstructionTime = record.getTimestamp();
                            }
                        }
                        if (record.getMessage().contains("Not there! please remove that block again")
                                || record.getMessage().contains("Please add this block again.")) {
                            numMistakes += 1;
                        }
                        if (record.getMessage().contains("Congratulations, you are done building")) {
                            // the game is complete, i.e. the last HLO was completed.
                            if (hloPlans.get(hloPlans.size() - 1).timestamp == null) {
                                hloPlans.get(hloPlans.size() - 1).timestamp = record.getTimestamp();
                                hloPlans.get(hloPlans.size() - 1).mistakes = numMistakes;
                            } else if (hloPlans.get(hloPlans.size() - 2).timestamp == null) {
                                //checkstyle needs something here
                            } else if (hloPlans.get(hloPlans.size() - 2).timestamp
                                            .until(hloPlans.get(hloPlans.size() - 1).timestamp, MILLIS) < 0) {
                                hloPlans.get(hloPlans.size() - 1).timestamp = record.getTimestamp();
                                hloPlans.get(hloPlans.size() - 1).mistakes = numMistakes;
                            }
                        }

                        break;
                    default:
                        break;
                }
                // Check which HLOs are complete
                for (HLOGatherer hlo : hloPlans) {
                    if (hlo.timestamp == null) {
                        if (presentBlocks.containsAll(hlo.blocks)) {
                            hlo.timestamp = record.getTimestamp();
                            hlo.mistakes = numMistakes;
                        }
                    }
                }
            }
            count++;
            // we are done when we either already had our second try or the data looks good.
            if (ignoreDestroyMessages || hloPlans.stream().noneMatch((x) -> x.timestamp == null)) {
                dataExtracted = true;
            } else {
                //there was some problem in the game with regard to block placement/destruction timing
                //so we extract the latest time at which a block in the hlo was placed since this
                // must be the time our system assumed the construction was finished,
                // regardless of the current state of the minecraft world
                if (wasSuccessful()) {
                    for (HLOGatherer hlo : hloPlans) {
                        if (hlo.timestamp == null) {
                            LocalDateTime latestTimestamp = firstInstructionTime;
                            for (Block b : hlo.blocks) {
                                LocalDateTime tmp = blocksTime.get(b);
                                if (tmp == null) {
                                    continue;
                                }
                                if (latestTimestamp == null) {
                                    latestTimestamp = tmp;
                                } else if (latestTimestamp.isBefore(tmp)) {
                                    latestTimestamp = tmp;
                                }
                            }
                            hlo.timestamp = latestTimestamp;
                        }
                    }
                }
                if (hloPlans.stream().noneMatch((x) -> x.timestamp == null)) {
                    dataExtracted = true;
                } else {
                    System.out.println("Some HLO is still not finished gameId: " + gameId);
                }
                ignoreDestroyMessages = true;
            }
        }

        // As this was a successful game, all HLOs should have a time
        assert (hloPlans.stream().noneMatch((x) -> x.timestamp == null));

        if (firstInstructionTime == null) {
            logger.error("first instruction is null");
            return List.of();
        }
        if (getScenario().equals("bridge")) {
            return List.of(
                    new Pair<>("floor",
                            new HLOInformation(
                                    (int) firstInstructionTime.until(hloPlans.get(0).timestamp, MILLIS),
                                    getNumMistakesInTimespan(firstInstructionTime, hloPlans.get(0).timestamp)
                            )
                    ),
                    new Pair<>("railing",
                            new HLOInformation(
                                    (int) hloPlans.get(0).timestamp.until(hloPlans.get(1).timestamp, MILLIS),
                                    getNumMistakesInTimespan(hloPlans.get(0).timestamp, hloPlans.get(1).timestamp)
                            )
                    ),
                    new Pair<>("railing",
                            new HLOInformation(
                                    (int) hloPlans.get(1).timestamp.until(hloPlans.get(2).timestamp, MILLIS),
                                    getNumMistakesInTimespan(hloPlans.get(1).timestamp, hloPlans.get(2).timestamp)
                            )
                    )
            );
        } else if (getScenario().equals("house")) {
            return List.of(
                    new Pair<>("wall",
                            new HLOInformation((int) firstInstructionTime.until(hloPlans.get(0).timestamp, MILLIS),
                                    hloPlans.get(0).mistakes
                            )
                    ),
                    new Pair<>("wall",
                            new HLOInformation((int) hloPlans.get(0).timestamp.until(hloPlans.get(1).timestamp, MILLIS),
                                    hloPlans.get(1).mistakes - hloPlans.get(0).mistakes
                            )
                    ),
                    new Pair<>("wall",
                            new HLOInformation((int) hloPlans.get(1).timestamp.until(hloPlans.get(2).timestamp, MILLIS),
                                    hloPlans.get(2).mistakes - hloPlans.get(1).mistakes
                            )
                    ),
                    new Pair<>("wall",
                            new HLOInformation((int) hloPlans.get(2).timestamp.until(hloPlans.get(3).timestamp, MILLIS),
                                    hloPlans.get(3).mistakes - hloPlans.get(2).mistakes
                            )
                    ),
                    new Pair<>("row",
                            new HLOInformation((int) hloPlans.get(3).timestamp.until(hloPlans.get(4).timestamp, MILLIS),
                                    hloPlans.get(4).mistakes - hloPlans.get(3).mistakes
                            )
                    ),
                    new Pair<>("row",
                            new HLOInformation((int) hloPlans.get(4).timestamp.until(hloPlans.get(5).timestamp, MILLIS),
                                    hloPlans.get(5).mistakes - hloPlans.get(4).mistakes
                            )
                    ),
                    new Pair<>("row",
                            new HLOInformation((int) hloPlans.get(5).timestamp.until(hloPlans.get(6).timestamp, MILLIS),
                                    hloPlans.get(6).mistakes - hloPlans.get(5).mistakes
                            )
                    ),
                    new Pair<>("row",
                            new HLOInformation((int) hloPlans.get(6).timestamp.until(hloPlans.get(7).timestamp, MILLIS),
                                    hloPlans.get(7).mistakes - hloPlans.get(6).mistakes
                            )
                    )
            );
        }
        return List.of();
    }

    /**
     * @return list of coordinates of blocks for each highlevelobject
     *     if instructions are per block
     */
    private List<List<Block>> readBlockPlan(String filename) {
        InputStream inputStream = GameInformation.class.getResourceAsStream(filename);
        String blockPlan = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String[] steps = blockPlan.split("\n");
        List<Block> currentBlocks = new ArrayList<>();
        List<List<Block>> hloPlans = new ArrayList<>();
        for (String step : steps) {
            if (step.contains("-starting")) {
                currentBlocks = new ArrayList<>();
            } else if (step.contains("!place-block")) {
                String[] stepArray = step.split(" ");
                int x = (int) Double.parseDouble(stepArray[2]);
                int y = (int) Double.parseDouble(stepArray[3]);
                int z = (int) Double.parseDouble(stepArray[4]);
                currentBlocks.add(new Block(x, y, z));
            } else if (step.contains("-finished")) {
                hloPlans.add(currentBlocks);
            }
        }
        return hloPlans;
    }

    /**
     * @return list of coordinates of blocks for each highlevelobject
     *     if instructions are highlevel
     */
    private List<List<Block>> readHighlevelPlan(String filename) {
        InputStream inputStream = GameInformation.class.getResourceAsStream(filename);
        String blockPlan = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String[] steps = blockPlan.split("\n");
        List<Block> currentBlocks = new ArrayList<>();
        List<List<Block>> hloPlans = new ArrayList<>();
        for (String step : steps) {
            if (step.contains("!build-")) {
                if (!currentBlocks.isEmpty()) {
                    hloPlans.add(currentBlocks);
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
        hloPlans.add(currentBlocks);
        return hloPlans;
    }

    /**
     * @return set of blocks given in the inital world
     */
    private Set<Block> readInitialWorld(String filename) {
        Set<Block> worldBlocks = new HashSet<>();
        InputStream inputStream = GameInformation.class.getResourceAsStream(filename);
        String blockDescriptions = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        String[] lines = blockDescriptions.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] blockInfo = line.split(",");
            int x = Integer.parseInt(blockInfo[0]);
            int y = Integer.parseInt(blockInfo[1]);
            int z = Integer.parseInt(blockInfo[2]);
            worldBlocks.add(new Block(x, y, z));
        }
        return worldBlocks;
    }

    /**
     * writes the results of the evaluation methods above to a provided markdown file.
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
        for (int current : blockDurations) {
            durations.append("\n - ").append(current).append("ms");
        }

        if (wasSuccessful) {
            List<Pair<String, HLOInformation>> hloDurations;
            if (this.hloInformation == null) {
                hloDurations = getHLOInformation();
                this.hloInformation = hloDurations;
            } else {
                hloDurations = this.hloInformation;
            }
            durations.append("\n\n# Durations per High-level object");
            for (var pair : hloDurations) {
                durations.append("\n - ").append(pair.getFirst());
                durations.append(" : ").append(pair.getSecond().duration);
                durations.append("ms");
            }

            getDurationPerInstruction();
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
     * prints three lists to the console.
     * - all blocks that were placed in this game until the given timestamp
     * - all blocks that were destroyed in this game until the given timestamp
     * - all blocks that were placed but not destroyed again until the given timestamp
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

        for (GameLogsRecord record : result) {
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
        for (Block block : placedBlocks) {
            System.out.println(" - " + block.toString());
        }

        System.out.println("Destroyed Blocks");
        for (Block block : destroyedBlocks) {
            System.out.println(" - " + block.toString());
        }

        System.out.println("Present Blocks");
        for (Block block : presentBlocks) {
            System.out.println(" - " + block.toString());
        }
    }

}
