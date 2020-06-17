package de.saar.minecraft.analysis;

import de.saar.minecraft.broker.db.Tables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExperimentAnalysis {

    private static final Logger logger = LogManager.getLogger(ExperimentAnalysis.class);
    private AnalysisConfiguration config;
    private DSLContext jooq;

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
     }

    public void makeScenarioAnalysis() throws IOException {
        Result<Record1<String>> result = jooq.selectDistinct(Tables.GAMES.SCENARIO)
                .from(Tables.GAMES)
                .fetch();
        List<String> scenarios = new ArrayList<>();
        for (var record: result) {
            scenarios.add(record.get(Tables.GAMES.SCENARIO));
        }
        Path basePath = Paths.get(config.getDirName(), "per_scenario");
        if (!basePath.toFile().isDirectory() && !basePath.toFile().mkdir()) {
            logger.error("Could not create directory " + basePath.toString());
            throw new IOException("Could not create directory " + basePath.toString());
        }
        for (String scenario: scenarios) {

            var info = new ScenarioInformation(scenario, this.jooq);
            String currentFileName = String.format("scenario-details-%s.md", scenario);
            File file = new File(String.valueOf(basePath), currentFileName);
            info.writeAnalysis(file);
        }
    }

    public void makeArchitectAnalysis() throws IOException {
        Result<Record1<String>> result = jooq.selectDistinct(Tables.GAMES.ARCHITECT_INFO)
                .from(Tables.GAMES)
                .fetch();
        List<String> architects = new ArrayList<>();
        for (var record: result) {
            architects.add(record.get(Tables.GAMES.ARCHITECT_INFO));
        }
        Path basePath = Paths.get(config.getDirName(), "per_architect");
        if (!basePath.toFile().isDirectory() && !basePath.toFile().mkdir()) {
            throw new IOException("Could not create directory " + basePath.toString());
        }
        for (String arch: architects) {
            var info = new ArchitectInformation(arch, this.jooq);
            String currentFileName = String.format("architect-details-%s.md", arch);
            File file = new File(String.valueOf(basePath), currentFileName);
            info.writeAnalysis(file);
        }

    }
}
