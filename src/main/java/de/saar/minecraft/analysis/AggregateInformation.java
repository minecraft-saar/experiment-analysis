package de.saar.minecraft.analysis;

import de.saar.minecraft.broker.db.Tables;
import de.saar.minecraft.broker.db.tables.records.GamesRecord;
import de.saar.minecraft.broker.db.tables.records.QuestionnairesRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

public class AggregateInformation {
    DSLContext jooq;
    List<GameInformation> games;

    /**
     * Should probably be deprecated
     */
    protected AggregateInformation() {}

    public AggregateInformation(List<GameInformation> games) {
        this.games = games;
    }

    private static final Logger logger = LogManager.getLogger(AggregateInformation.class);

    public int getNumGames() {
        return games.size();
    }

    public float getAverageGameDuration() {
        List<Long> durations = new ArrayList<>();
        for (GameInformation info: games) {
            if (info.wasSuccessful()) {
                durations.add(info.getTimeToSuccess());
            }
        }
        float sum = (float)durations.stream().reduce((long) 0, Long::sum);
        return sum / durations.size();
    }

    public float getFractionSuccessfulGames() {
        int numSuccessFull = 0;
        for (GameInformation info: games) {
            if (info.wasSuccessful()) {
                numSuccessFull++;
            }
        }
        return (float)numSuccessFull / games.size();
    }

    public float getAverageNumMistakes() {
        int totalMistakes = 0;
        for (GameInformation info: games) {
            totalMistakes += info.getNumMistakes();
        }
        return (float)totalMistakes / games.size();
    }

    public float getAverageNumBlocksPlaced() {
        int totalBlocks = 0;
        for (GameInformation info: games) {
            totalBlocks += info.getNumBlocksPlaced();
        }
        return (float)totalBlocks / games.size();
    }

    public float getAverageNumBlocksDestroyed() {
        int totalBlocks = 0;
        for (GameInformation info: games) {
            totalBlocks += info.getNumBlocksDestroyed();
        }
        return (float)totalBlocks / games.size();
    }

    /**
     * Fraction of players that made at least one mistake
     * @return
     */
    public float getFractionMistakes() {
        int withMistakes = 0;
        for (GameInformation info: games) {
            if (info.getNumMistakes() > 0) {
                withMistakes++;
            }
        }
        return (float)withMistakes / games.size();
    }

    public HashMap<Integer,Integer> getMistakeDistribution() {
        HashMap<Integer, Integer> distribution = new HashMap<>();
        for (GameInformation info: games) {
            int mistakes = info.getNumMistakes();
            distribution.put(mistakes, distribution.getOrDefault(mistakes, 0) + 1);
        }
        return distribution;
    }

    public List<Answer> getAnswerDistribution() {
        HashMap<String, DescriptiveStatistics> collection = new HashMap<>();

        for (GameInformation info: games) {
            // collect
            for (Pair<String, Integer> qa: info.getNumericQuestions()) {
                int currentAnswer;
                // only include numeric answers
                collection.putIfAbsent(qa.getFirst(), new DescriptiveStatistics());
                collection.get(qa.getFirst()).addValue(qa.getSecond());
            }
        }

        // compute distribution
        List<Answer> distribution = new ArrayList<>();
        for (String question: collection.keySet()) {

            DescriptiveStatistics statistics = collection.get(question);
            double mean = statistics.getGeometricMean();
            double stdDeviation = statistics.getStandardDeviation();
            int median = (int) statistics.getPercentile(50);
            int minimum = (int) statistics.getMin();
            int maximum = (int) statistics.getMax();
            distribution.add(new Answer(question, mean, stdDeviation, median, minimum, maximum));
        }
        return distribution;
    }

    public HashMap<String,List<String>> getAllFreeTextResponses() {
        HashMap<String, List<String>> collection = new HashMap<>();
        for (GameInformation info: games) {
            for (Pair<String, String> qa : info.getFreeformQuestions()) {
                var question = qa.getFirst();
                var answer = qa.getSecond();
                collection.putIfAbsent(question, new ArrayList<>());
                if (!answer.isEmpty()) {
                    collection.get(question).add(answer);
                }
            }
        }
        return collection;
    }

    public List<Pair<String,Integer>> getAverageDurationPerHLO() {
        // TODO: only works if all games have the same scenario
        List<Pair<String, List<Integer>>> addedDurations = new ArrayList<>();
        for (GameInformation info: games) {
            if (!info.wasSuccessful()) {
                continue;
            }
            var current = info.getDurationPerHLO();
            if (addedDurations.isEmpty()) {
                addedDurations = current.stream()
                        .map((x -> new Pair<String, List<Integer>>(x.getFirst(), new ArrayList<>())))
                        .collect(Collectors.toList());
            }
            for (int i = 0; i < current.size(); i++) {
                String objectName = current.get(i).getFirst();
                int newDuration = current.get(i).getSecond();
                if (!addedDurations.get(i).getFirst().equals(objectName)){
                    logger.error("wrong high-level object");
                }
                var durations = addedDurations.get(i).getSecond();
                durations.add(newDuration);
            }
        }
        return addedDurations.stream()
                .map((x) -> new Pair<>(
                        x.getFirst(),(x.getSecond().stream().reduce(0, Integer::sum)) / x.getSecond().size()))
                .collect(Collectors.toList());
    }

    public void writeAnalysis(File file) throws IOException {
        FileWriter writer = new FileWriter(file);

        String overview = "# Overview" + "\n - Number of games: " +
                getNumGames() +
                "\n - Average game duration: " +
                getAverageGameDuration() +
                "\n - Fraction of successful games: " +
                getFractionSuccessfulGames() +
                "\n - Fraction of players making a mistake: " +
                getFractionMistakes() +
                "\n - Average number of mistakes: " +
                getAverageNumMistakes() +
                "\n - Average number of blocks placed: " +
                getAverageNumBlocksPlaced() +
                "\n - Average number of blocks destroyed: " +
                getAverageNumBlocksDestroyed() +
                "\n\n";
        writer.write(overview);

        StringBuilder gameList = new StringBuilder("\nGames in this category\n");
        for (GameInformation gi: games) {
            gameList.append(gi.gameId).append(", ");
        }
        writer.write(gameList.toString());


        StringBuilder likert = new StringBuilder("\n\n# Likert Questions\n");

        likert.append("| Question | Mean | Standard Deviation | Median | Minimum | Maximum |\n");
        likert.append("| -------- | ----:| ------------------:| ------:| -------:| -------:|\n");
        for (Answer answer: getAnswerDistribution()){
            likert.append (String.format("%s | %.2f | %.2f | %d | %d | %d |\n",
                    answer.getQuestion(),
                    answer.getMean(),
                    answer.getStdDeviation(),
                    answer.getMedian(),
                    answer.getMinimum(),
                    answer.getMaximum()));
        }
        writer.write(likert.toString());
        StringBuilder free = new StringBuilder("\n\n# Free-form Questions");
        for (Map.Entry<String, List<String>> entry : getAllFreeTextResponses().entrySet()) {
            String question = entry.getKey();
            List<String> answers = entry.getValue();
            free.append("\n### ").append(question);
            for (String answer: answers) {
                free.append("\n - ").append(answer);
            }
        }
        writer.write(free.toString());
        writer.flush();
        StringBuilder hloDurations = new StringBuilder("\n\n# Average Duration per HLO");
        for (var duration: getAverageDurationPerHLO()) {
            hloDurations.append("\n - ").append(duration.getFirst());
            hloDurations.append(": ").append(duration.getSecond());
        }
        writer.write(hloDurations.toString());
        writer.close();
    }

    public static class Answer {
        private String question;
        private double mean;
        private double stdDeviation;
        private int median;
        private int minimum;
        private int maximum;

        public Answer(String question, double mean, double stdDeviation, int median, int minimum,
            int maximum) {
            this.question = question;
            this.mean = mean;
            this.stdDeviation = stdDeviation;
            this.median = median;
            this.minimum = minimum;
            this.maximum = maximum;
        }

        public String getQuestion() {
            return question;
        }

        public double getMean() {
            return mean;
        }

        public double getStdDeviation() {
            return stdDeviation;
        }

        public int getMedian() {
            return median;
        }

        public int getMinimum() {
            return minimum;
        }

        public int getMaximum() {
            return maximum;
        }
    }
}
