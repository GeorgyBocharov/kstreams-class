package ru.curs.homework.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;
import ru.curs.counting.model.Score;

@Slf4j
public class ScoreToWinningBetTransformer implements StatefulTransformer<String, Score, String, EventScore, String, Bet> {
    @Override
    public String storeName() {
        return "WINNING_BETS";
    }

    @Override
    public Serde<String> storeKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Score> storeValueSerde() {
        return new JsonSerde<>(Score.class);
    }

    @Override
    public KeyValue<String, Bet> transform(String key, EventScore eventScore, KeyValueStore<String, Score> stateStore) {
        Score lastScore = stateStore.get(key);
        Score currentScore = eventScore.getScore();
        long time = eventScore.getTimestamp();
        stateStore.put(key, currentScore);
        if (lastScore == null) {
            if (currentScore.getHome() > currentScore.getAway()) {
                return createWinningBet(key, Outcome.H, time);
            } else if (currentScore.getHome() < currentScore.getAway()) {
                return createWinningBet(key, Outcome.A, time);
            }
            log.error("Got incorrect eventScore: {}", eventScore);
            return KeyValue.pair("INCORRECT-SCORE", null);
        }
        if (currentScore.getHome() > lastScore.getHome()) {
            return createWinningBet(key, Outcome.H, time);
        } else if (currentScore.getAway() > lastScore.getAway()) {
            return createWinningBet(key, Outcome.A, time);
        }
        log.error("Got incorrect eventScore: {}", eventScore);
        return KeyValue.pair("INCORRECT-SCORE", null);
    }

    private KeyValue<String, Bet> createWinningBet(String key, Outcome outcome, long time) {
        log.info("Creating winning bet for key {}, outcome {}", key, outcome);
        return KeyValue.pair(
                String.format("%s:%s", key, outcome),
                Bet.builder()
                        .match(key)
                        .outcome(outcome)
                        .timestamp(time)
                        .build()
        );
    }
}
