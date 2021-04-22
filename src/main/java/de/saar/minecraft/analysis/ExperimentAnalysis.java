package de.saar.minecraft.analysis;

import de.saar.minecraft.broker.db.Tables;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;


public class ExperimentAnalysis {

    private static final Logger logger = LogManager.getLogger(ExperimentAnalysis.class);
    private final AnalysisConfiguration config;
    private DSLContext jooq;
    private final List<String> scenarios;
    private final List<String> architects;
    private final List<GameInformation> gameInformations;

    public ExperimentAnalysis(AnalysisConfiguration config) {
        this.config = config;
        var url = config.getUrl();
        var user = config.getUser();
        var password = config.getPassword();

        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            DSLContext ret = DSL.using(
                    conn,
                    SQLDialect.valueOf("MYSQL")
            );
            logger.info("Connected to database at {}.", url);
            this.jooq = ret;
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        scenarios = jooq.selectDistinct(Tables.GAMES.SCENARIO)
                .from(Tables.GAMES)
                .fetch(Tables.GAMES.SCENARIO);
        architects = jooq.selectDistinct(Tables.GAMES.ARCHITECT_INFO)
                .from(Tables.GAMES)
                .fetch(Tables.GAMES.ARCHITECT_INFO);
        gameInformations = jooq.selectFrom(Tables.GAMES)
                .orderBy(Tables.GAMES.ID.asc())
                .fetchStream()
                .map((x) -> new GameInformation(x.getId(), jooq))
                .collect(Collectors.toList());
    }

    public void makeAnalysis() throws IOException {
        String dirName = config.getDirName();
        if (! new File(dirName).isDirectory()) {
            boolean wasCreated = new File(dirName).mkdir();
            if (!wasCreated) {
                logger.error("Output directory {} could not be created", dirName);
                return;
            }
        }
        makeScenarioAnalysis();
        makeArchitectAnalysis();
        makeGameAnalyses();
        saveAsCSV();

        for (var scenario: scenarios) {
            for (var architect: architects) {
                var gamedata = gameInformations.stream()
                        .filter((gi) -> {
                            if (gi.getArchitect() == null) {
                                return false;
                            }
                            return gi.getArchitect().equals(architect);
                        })
                        .filter((gi) -> gi.getScenario().equals(scenario))
                        .collect(Collectors.toList());
                makeAnalysis(scenario + "-" + architect + ".md", gamedata);
            }
        }

    }

    public void makeAnalysis(String analysisName, List<GameInformation> gi) throws IOException {
        File file = new File(config.getDirName(), analysisName);
        var info = new AggregateInformation(gi);
        info.writeAnalysis(file);
    }

    public void makePartialAnalysis(String scenario, String architect, boolean onlySuccessful) throws IOException {
        List<GameInformation> gamedata = gameInformations;

        if (architect != null) {
            gamedata = gamedata.stream()
                    .filter((gi) -> {
                        if (gi.getArchitect() == null) {
                            return false;
                        }
                        return gi.getArchitect().equals(architect);
                    })
                    .collect(Collectors.toList());
        }

        if (scenario != null) {
            gamedata = gamedata.stream()
                    .filter((gi) -> gi.getScenario().equals(scenario))
                    .collect(Collectors.toList());
        }

        if (onlySuccessful) {
            gamedata = gamedata.stream()
                    .filter(GameInformation::wasSuccessful)
                    .collect(Collectors.toList());
        }

        makeAnalysis(scenario + "-" + architect + "-" + onlySuccessful + ".md", gamedata);
    }

    public void makeScenarioAnalysis() throws IOException {
        Path basePath = Paths.get(config.getDirName(), "per_scenario");
        if (!basePath.toFile().isDirectory() && !basePath.toFile().mkdir()) {
            logger.error("Could not create directory " + basePath.toString());
            throw new IOException("Could not create directory " + basePath.toString());
        }
        for (String scenario: scenarios) {
            var info = new AggregateInformation(
                    gameInformations
                            .stream()
                            .filter((x) -> x.getScenario().equals(scenario))
                            .collect(Collectors.toList()));
            String currentFileName = String.format("scenario-details-%s.md", scenario);
            File file = new File(String.valueOf(basePath), currentFileName);
            info.writeAnalysis(file);
        }
    }

    public void makeArchitectAnalysis() throws IOException {
        Path basePath = Paths.get(config.getDirName(), "per_architect");
        if (!basePath.toFile().isDirectory() && !basePath.toFile().mkdir()) {
            throw new IOException("Could not create directory " + basePath.toString());
        }
        for (String arch: architects) {
            var info = new AggregateInformation(
                    gameInformations
                            .stream()
                            .filter((x) -> {
                                if (x.getArchitect() == null) {
                                    return false;
                                }
                                return x.getArchitect().equals(arch);
                            })
                            .collect(Collectors.toList()), true);
            String currentFileName = String.format("architect-details-%s.md", arch);
            File file = new File(String.valueOf(basePath), currentFileName);
            info.writeAnalysis(file);
        }
    }

    public void makeGameAnalyses() throws IOException {
        Path basePath = Paths.get(config.getDirName(), "per_game");
        if (!basePath.toFile().isDirectory() && !basePath.toFile().mkdir()) {
            throw new IOException("Could not create directory " + basePath.toString());
        }
        for (GameInformation info: gameInformations) {
            String filename = String.format("game-%d.md", info.gameId);
            File file = new File(basePath.toString(), filename);
            info.writeAnalysis(file);
        }
    }

    public void makeGameAnalysis(int gameId) throws IOException {
        Path basePath = Paths.get(config.getDirName(), "per_game");
        if (!basePath.toFile().isDirectory() && !basePath.toFile().mkdir()) {
            throw new IOException("Could not create directory " + basePath.toString());
        }
        GameInformation info = gameInformations.stream()
                .filter((x) -> x.gameId == gameId)
                .findFirst()
                .orElse(null);
        String filename = String.format("game-%d.md", info.gameId);
        File file = new File(basePath.toString(), filename);
        info.writeAnalysis(file);
        // info.printBlocksUntilTimestamp(LocalDateTime.of(2020,6,15, 11,17,25));
    }

    /**
     * Saves all game data into a CSV file "data.csv".
     * @throws IOException
     */
    public void saveAsCSV() throws IOException {
        File file = new File(config.getDirName(), "data.csv");
        // only save complete games with questionnaire
        var gi = gameInformations.stream()
                .filter((x) -> ! x.getNumericQuestions().isEmpty())
                .collect(Collectors.toList());
        var info = new AggregateInformation(gi);
        if (info.games.isEmpty()) {
            logger.warn("There are no complete games with questionnaire, not saving a CSV file!");
            return;
        }
        info.saveCSV(file);
    }
}
