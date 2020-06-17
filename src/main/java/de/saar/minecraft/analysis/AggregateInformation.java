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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

public class AggregateInformation {
    DSLContext jooq;
    List<GameInformation> games;

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
        return roundTwoDecimals(sum / durations.size());
    }

    public float getFractionSuccessfulGames() {
        int numSuccessFul = 0;
        for (GameInformation info: games) {
            if (info.wasSuccessful()) {
                numSuccessFul++;
            }
        }
        return roundTwoDecimals((float)numSuccessFul / games.size());
    }

    public float getAverageNumMistakes() {
        int totalMistakes = 0;
        for (GameInformation info: games) {
            // TODO: count all games or only successful games?
            totalMistakes += info.getNumMistakes();
        }
        return roundTwoDecimals((float)totalMistakes / games.size());
    }

    public float getAverageNumBlocksPlaced() {
        int totalBlocks = 0;
        for (GameInformation info: games) {
            // TODO: count all games or only successful games?
            totalBlocks += info.getNumBlocksPlaced();
        }
        return roundTwoDecimals((float)totalBlocks / games.size());
    }

    public float getAverageNumBlocksDestroyed() {
        int totalBlocks = 0;
        for (GameInformation info: games) {
            // TODO: count all games or only successful games?
            totalBlocks += info.getNumBlocksDestroyed();
        }
        return roundTwoDecimals( (float)totalBlocks / games.size() );
    }

    /**
     * Fraction of players that made at least one mistake
     * @return
     */
    public float getFractionMistakes() {
        int withMistakes = 0;
        for (GameInformation info: games) {
            // TODO: count all games or only successful games?
            if (info.getNumMistakes() > 0) {
                withMistakes++;
            }
        }
        return roundTwoDecimals((float)withMistakes / games.size());
    }

    public HashMap<Integer,Integer> getMistakeDistribution() {
        HashMap<Integer, Integer> distribution = new HashMap<>();
        for (GameInformation info: games) {
            int mistakes = info.getNumMistakes();
            distribution.put(mistakes, distribution.getOrDefault(mistakes, 0) + 1);
        }
        return distribution;
    }

    private float roundTwoDecimals(float d) {
        DecimalFormat twoDForm = new DecimalFormat("#.##");
        return Float.parseFloat(twoDForm.format(d));
    }

    public List<Answer> getAnswerDistribution() {
//        HashMap<String, List<Integer>> collection = new HashMap<>();
        HashMap<String, DescriptiveStatistics> collection = new HashMap<>();

        for (GameInformation info: games) {
            int gameId = info.gameId;
            Result<QuestionnairesRecord> questionnaire = jooq.selectFrom(Tables.QUESTIONNAIRES)
                .where(Tables.QUESTIONNAIRES.GAMEID.equal(gameId))
                .orderBy(Tables.QUESTIONNAIRES.ID.asc())
                .fetch();
            // collect
            for (QuestionnairesRecord row: questionnaire) {
                int currentAnswer;
                // only include numeric answers
                if (NumberUtils.isDigits(row.getAnswer())) {
                    currentAnswer = Integer.parseInt(row.getAnswer());
                    String question = row.getQuestion();
                    collection.putIfAbsent(question, new DescriptiveStatistics());
                    collection.get(question).addValue(currentAnswer);
                }
            }
        }

        // compute distribution
        List<Answer> distribution = new ArrayList<>();
        for (String question: collection.keySet()) {
//            List<Integer> rawAnswers = collection.get(question);
//            float average = (float)rawAnswers.stream().reduce(0, Integer::sum) / rawAnswers.size();
//            int mode;
//            int median;
//            int minimum = rawAnswers.stream().reduce(100, Integer::min);
//            int maximum = rawAnswers.stream().reduce(0, Integer::max);;

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
            int gameId = info.gameId;
            Result<QuestionnairesRecord> questionnaire = jooq.selectFrom(Tables.QUESTIONNAIRES)
                .where(Tables.QUESTIONNAIRES.GAMEID.equal(gameId))
                .orderBy(Tables.QUESTIONNAIRES.ID.asc())
                .fetch();
            for (QuestionnairesRecord row : questionnaire) {
                if (!NumberUtils.isDigits(row.getAnswer())) {
                    String answer = row.getAnswer();
                    String question = row.getQuestion();
                    collection.putIfAbsent(question, new ArrayList<>());
                    if (!answer.isEmpty()) {
                        collection.get(question).add(answer);
                    }
                }
            }
        }
        return collection;
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

        StringBuilder likert = new StringBuilder("\n\n# Likert Questions\n");

        likert.append("| Question | Mean | Standard Deviation | Median | Minimum | Maximum |\n");
        likert.append("| -------- | ----:| ------------------:| ------:| -------:| -------:|\n");
        for (Answer answer: getAnswerDistribution()){
            likert.append("| ").append(answer.getQuestion());
            likert.append(" | ").append(answer.getMean());
            likert.append(" | ").append(answer.getStdDeviation());
            likert.append(" | ").append(answer.getMedian());
            likert.append(" | ").append(answer.getMinimum());
            likert.append(" | ").append(answer.getMaximum());
            likert.append("| \n");

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
            DecimalFormat twoDForm = new DecimalFormat("#.##");
            return Double.parseDouble(twoDForm.format(mean));
        }

        public double getStdDeviation() {
            DecimalFormat twoDForm = new DecimalFormat("#.##");
            return Double.parseDouble(twoDForm.format(stdDeviation));
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
