package de.saar.minecraft.analysis;

import de.saar.minecraft.broker.db.Tables;
import de.saar.minecraft.broker.db.tables.records.GamesRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import java.util.ArrayList;

public class ArchitectInformation extends AggregateInformation {

    private static final Logger logger = LogManager.getLogger(ArchitectInformation.class);

    public ArchitectInformation(String architect, DSLContext jooq) {
        this.jooq = jooq;
        games = new ArrayList<>();
        Result<GamesRecord> result = jooq.selectFrom(Tables.GAMES)
                .where(Tables.GAMES.ARCHITECT_INFO.equal(architect))
                .orderBy(Tables.GAMES.ID.asc())
                .fetch();

        for (GamesRecord record: result) {
            games.add(new GameInformation(record.getId(), jooq));
        }
    }
}
