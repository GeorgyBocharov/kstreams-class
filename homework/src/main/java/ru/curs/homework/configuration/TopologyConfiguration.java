package ru.curs.homework.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Fraud;
import ru.curs.counting.model.Outcome;
import ru.curs.homework.util.ScoreToWinningBetTransformer;

import java.time.Duration;

import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    public static final String BETTOR_AMOUNTS = "bettor-amounts";
    public static final String TEAM_AMOUNTS = "team-amounts";
    public static final String POSSIBLE_FRAUDS = "possible-frauds";

    private final ScoreToWinningBetTransformer scoreToWinningBetTransformer = new ScoreToWinningBetTransformer();

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {

        /*
        Необходимо создать топологию, которая имеет следующие три выходных топика:
           -- таблица, ключом которой является имя игрока,
           а значением -- сумма ставок, произведённых игроком
           -- таблица, ключом которой является имя команды,
            а значением -- сумма ставок на эту команду (ставки на "ничью" в подсчёте игнорируются)
           -- поток, ключом которого является имя игрока,
           а значениями -- подозрительные ставки.
           Подозрительными считаем ставки, произведённые в пользу команды
           в пределах одной секунды до забития этой командой гола.
         */

        KStream<String, Bet> betStream = streamsBuilder
                .stream(
                        BET_TOPIC,
                        Consumed
                                .with(Serdes.String(), new JsonSerde<>(Bet.class))
                                .withTimestampExtractor(
                                        (record, timestamp) -> ((Bet) record.value()).getTimestamp()
                                )
                );

        betterAmountsCalculation(betStream);

        teamAmountCalculation(betStream);

        fraudCalculation(streamsBuilder, betStream);

        Topology topology = streamsBuilder.build();
        System.out.println("==============================");
        System.out.println(topology.describe());
        System.out.println("==============================");
        // https://zz85.github.io/kafka-streams-viz/
        return topology;
    }

    private void betterAmountsCalculation(KStream<String, Bet> betStream) {
        betStream
                .map((betId, bet) -> KeyValue.pair(bet.getBettor(), bet.getAmount()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> newValue + aggValue,
                        Materialized.with(Serdes.String(), Serdes.Long())
                )
                .toStream()
                .to(BETTOR_AMOUNTS);
    }

    private void teamAmountCalculation(KStream<String, Bet> betStream) {
        betStream
                .filter((betId, bet) -> !betId.endsWith(Outcome.D.name()))
                .map((betId, bet) -> KeyValue.pair(getTeamFromBetId(betId), bet.getAmount()))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> newValue + aggValue,
                        Materialized.with(Serdes.String(), Serdes.Long())
                ).toStream().to(TEAM_AMOUNTS);
    }

    private String getTeamFromBetId(String betId) {
        return betId.endsWith(Outcome.H.name()) ?
                betId.substring(0, betId.indexOf('-')) :
                betId.substring(betId.indexOf('-') + 1, betId.indexOf(':'));
    }

    private void fraudCalculation(StreamsBuilder streamsBuilder, KStream<String, Bet> betStream) {
        KStream<String, EventScore> eventScoreKStream = streamsBuilder.stream(
                EVENT_SCORE_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class)).withTimestampExtractor(
                        (record, timestamp) -> ((EventScore) record.value()).getTimestamp()
                )
        );

        KStream<String, Bet> winningBets = scoreToWinningBetTransformer.transformStream(streamsBuilder, eventScoreKStream);

        betStream
                .join(
                        winningBets,
                        this::createFraudFromBets,
                        JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                        StreamJoined.with(Serdes.String(), new JsonSerde<>(Bet.class), new JsonSerde<>(Bet.class))
                )
                .map((betId, fraud) -> KeyValue.pair(fraud.getBettor(), fraud))
                .to(POSSIBLE_FRAUDS, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));
    }

    private Fraud createFraudFromBets(Bet bet, Bet winningBet) {
        return Fraud.builder()
                .bettor(bet.getBettor())
                .amount(bet.getAmount())
                .outcome(bet.getOutcome())
                .odds(bet.getOdds())
                .match(bet.getMatch())
                .lag(winningBet.getTimestamp() - bet.getTimestamp())
                .build();
    }
}
