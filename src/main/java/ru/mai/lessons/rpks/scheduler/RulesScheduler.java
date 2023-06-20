package ru.mai.lessons.rpks.scheduler;

import com.typesafe.config.Config;
import lombok.Getter;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Getter
public class RulesScheduler {
    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private final RulesUpdater checker = new RulesUpdater();

    public void runScheduler(Config config) throws SQLException {
        checker.rules = new ArrayList<>();
        checker.initReader(config);
        service.scheduleAtFixedRate(checker, 0,
                config.getInt("application.updateIntervalSec"), TimeUnit.SECONDS);
    }

    public List<Rule> getRules() {
        return checker.getRules();
    }
}
