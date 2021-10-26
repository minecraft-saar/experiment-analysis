package de.saar.minecraft.analysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


@Command(name = "experiment-analysis", mixinStandardHelpOptions = true, version = "1.0",
        description = "Computes statistics for Minecraft experiments with the scenarios 'bridge' "
                + "and 'house'")
class Main implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(Main.class);

    @Option(names = {"--saveCSV"}, description = "Creates an overview csv file")
    private boolean saveCSV = false;

    @Option(names = "--singleGameAnalysis", description = "Runs the analysis for the game with "
            + "<gameId>")
    private Integer gameId = null;

    @Option(names = "--analysisFrom", description = "Runs the analysis for the games with IDs between "
            + "<startId>" + "<endID>")
    private Integer startID = null;

    @Option(names = "--analysisTo", description = "Runs the analysis for the games with IDs between "
            + "<startId>" + "<endID>")
    private Integer endID = null;

    @Option(names = "--fullAnalysis", description = "Runs the entire experiment analysis which "
            + "includes aggregate analyses per scenario, architect and every scenario-architect "
            + "combination, an analysis for each game and a csv file")
    private boolean fullAnalysis = false;

    @Option(names = "--allGamesAnalysis", description = "Runs a game analysis for every game in "
            + "the database")
    private boolean allGames = false;

    @Option(names = "--architectAnalysis", description = "Runs an aggregate analysis for each "
            + "architect in the database")
    private boolean architectAnalysis = false;

    @Option(names = "--scenarioAnalysis", description = "Runs an aggregate analysis for each "
            + "scenario in the database")
    private boolean scenarioAnalysis = false;

    @Option(names = "--partialAnalysis", arity = "3", description = "Runs an aggregate analysis "
            + "for games with the properties <scenario> <architect> <only successful (true/false)>")
    private String[] partialArguments;

    public static void main(String... args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws IOException {
        AnalysisConfiguration config;
        try {
            config = AnalysisConfiguration.loadYaml(new FileReader("config.yml"));
        } catch (FileNotFoundException e) {
            logger.error("Configuration file not found. {}", e.getMessage());
            return 1;
        }

        var experimentAnalysis = new ExperimentAnalysis(config);
        if (fullAnalysis) {
            logger.info("Starting full analysis");
            experimentAnalysis.makeAnalysis();
            logger.info("Full analysis finished.");
        }
        if(startID != null && endID != null){
            String dirName = config.getDirName();
            if (! new File(dirName).isDirectory()) {
                boolean wasCreated = new File(dirName).mkdir();
                if (!wasCreated) {
                    logger.error("Output directory {} could not be created", dirName);
                    System.exit(-1);
                }
            }
            for(int id = startID; id <= endID; id++){
                logger.info("Starting analysis for game {}", id);
                experimentAnalysis.makeGameAnalysis(id);
            }
            logger.info("Analysis finished.");
            logger.info("Saving in csv");
            experimentAnalysis.saveAsCSV(startID, endID);
        }

        if (scenarioAnalysis) {
            logger.info("Starting scenario analysis");
            experimentAnalysis.makeScenarioAnalysis();
            logger.info("Scenario analysis finished");
        }
        if (architectAnalysis) {
            logger.info("Starting architect analysis");
            experimentAnalysis.makeArchitectAnalysis();
            logger.info("Architect Analysis finished");
        }
        if (allGames) {
            logger.info("Starting game analysis for all games");
            experimentAnalysis.makeGameAnalyses();
            logger.info("Game analyses finished");
        }
        if (partialArguments != null) {
            logger.info("Starting partial analysis for combination {}", (Object) partialArguments);
            experimentAnalysis.makePartialAnalysis(partialArguments[0], partialArguments[1],
                    Boolean.parseBoolean(partialArguments[2]));
            logger.info("Partial analysis finished");
        }
        if (gameId != null) {
            logger.info("Starting analysis for game {}", gameId);
            experimentAnalysis.makeGameAnalysis(gameId);
            logger.info("Analysis finished.");
        }

        if (saveCSV) {
            logger.info("Starting saving CSV");
            experimentAnalysis.saveAsCSV();
            logger.info("CSV saved");
        }
        return 0;
    }
}
