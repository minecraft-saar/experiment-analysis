package de.saar.minecraft.analysis;

import de.saar.minecraft.broker.db.Tables;
import de.saar.minecraft.broker.db.tables.records.GamesRecord;
import org.jooq.DSLContext;
import org.jooq.Result;

import java.util.ArrayList;

public class ScenarioInformation extends AggregateInformation {

    public ScenarioInformation(String scenario, DSLContext jooq) {
        super();
        this.jooq = jooq;
        games = new ArrayList<>();
        Result<GamesRecord> result = jooq.selectFrom(Tables.GAMES)
                .where(Tables.GAMES.SCENARIO.equal(scenario))
                .orderBy(Tables.GAMES.ID.asc())
                .fetch();
        for (GamesRecord record: result) {
            games.add(new GameInformation(record.getId(), jooq));
        }
    }
}
