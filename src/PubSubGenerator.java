import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PubSubGenerator {
    private static final List<String> CITIES = Arrays.asList("Bucuresti", "Cluj", "Timisoara", "Iasi", "Constanta");

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        // Load config from JSON
        Config config = new Config("config.json");

        int numThreads = config.numThreads;
        int numPublications = config.numPublications;
        int numSubscriptions = config.numSubscriptions;
        int equalityRatio = config.equalityRatio;
        Map<String, Integer> fieldFrequencies = config.fieldFrequencies;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long startTime = System.currentTimeMillis();

        List<Future<List<String>>> pubFutures = new ArrayList<>();
        int perThreadPub = numPublications / numThreads;
        int threadBatchPub = numPublications - (perThreadPub * (numThreads - 1));
        for (int i = 0; i < numThreads; i++) {
            int count = (i == numThreads - 1) ? threadBatchPub : perThreadPub;
            pubFutures.add(executor.submit(() -> {
                List<String> pubs = new ArrayList<>(count);
                for (int j = 0; j < count; j++) {
                    pubs.add(new Publication().toString());
                }
                return pubs;
            }));
        }

        List<Future<List<String>>> subFutures = new ArrayList<>();
        int perThreadSub = numSubscriptions / numThreads;
        int threadBatchSub = numSubscriptions - (perThreadSub * (numThreads - 1));
        for (int i = 0; i < numThreads; i++) {
            int count = (i == numThreads - 1) ? threadBatchSub : perThreadSub;
            subFutures.add(executor.submit(() -> {
                List<String> subs = new ArrayList<>(count);
                for (int j = 0; j < count; j++) {
                    subs.add(new Subscription(fieldFrequencies, equalityRatio).toString());
                }
                return subs;
            }));
        }

        List<String> publications = new ArrayList<>(numPublications);
        for (Future<List<String>> future : pubFutures) {
            publications.addAll(future.get());
        }
        List<String> subscriptions = new ArrayList<>(numSubscriptions);
        for (Future<List<String>> future : subFutures) {
            subscriptions.addAll(future.get());
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");

        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            System.err.println("Pool did not terminate");
        }

        Files.write(Paths.get("publications.txt"), publications);
        Files.write(Paths.get("subscriptions.txt"), subscriptions);

        verifyPubsSubs(publications, subscriptions, fieldFrequencies, equalityRatio);
    }

    private static void verifyPubsSubs(List<String> publications,
                                       List<String> subscriptions,
                                       Map<String, Integer> fieldFrequencies,
                                       double equalityRatio) {
        Map<String, Integer> fieldCounts = new HashMap<>();
        int cityCount = 0;
        int cityEqualCount = 0;

        for (String sub : subscriptions) {
            String[] conditions = sub.substring(1, sub.length() - 1).split(";");
            for (String cond : conditions) {
                String[] parts = cond.substring(1, cond.length() - 1).split(",");
                String field = parts[0];
                fieldCounts.put(field, fieldCounts.getOrDefault(field, 0) + 1);

                if (field.equals("city")) {
                    cityCount++;
                    if (parts[1].equals("=")) {
                        cityEqualCount++;
                    }
                }
            }
        }

        System.out.println("\nVerification Results:");
        System.out.printf("Total publications: %d%n", publications.size());
        System.out.printf("Total subscriptions: %d%n", subscriptions.size());

        for (Map.Entry<String, Integer> entry : fieldFrequencies.entrySet()) {
            String field = entry.getKey();
            int expected = (int) Math.round(subscriptions.size() * entry.getValue() / 100.0);
            int actual = fieldCounts.getOrDefault(field, 0);
            System.out.printf("> %s: expected ~%d, actual %d (%.1f%%)%n",
                    field, expected, actual, (actual * 100.0 / subscriptions.size()));
        }

        if (cityCount > 0) {
            double actualRatio = cityEqualCount * 100.0 / cityCount;
            System.out.printf("city equality ratio: expected %.1f%%, actual %.1f%% (%d/%d)%n",
                    equalityRatio * 100, actualRatio, cityEqualCount, cityCount);
        }
    }

    static class Publication {
        private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
        private static final List<String> DIRECTIONS = Arrays.asList("N", "NE", "E", "SE", "S", "SW", "W", "NW");
        private static final int MIN_TEMP = -20;
        private static final int MAX_TEMP = 40;
        private static final double MIN_RAIN = 0.0;
        private static final double MAX_RAIN = 10.0;
        private static final int MIN_WIND = 0;
        private static final int MAX_WIND = 100;
        private static final int MIN_STATION_ID = 1;
        private static final int MAX_STATION_ID = 100;

        int stationId;
        String city;
        int temp;
        double rain;
        int wind;
        String direction;
        String date;

        public Publication() {
            this.stationId = RANDOM.nextInt(MIN_STATION_ID, MAX_STATION_ID + 1);
            this.city = CITIES.get(RANDOM.nextInt(CITIES.size()));
            this.temp = RANDOM.nextInt(MIN_TEMP, MAX_TEMP + 1);
            this.rain = RANDOM.nextDouble(MIN_RAIN, MAX_RAIN);
            this.wind = RANDOM.nextInt(MIN_WIND, MAX_WIND + 1);
            this.direction = DIRECTIONS.get(RANDOM.nextInt(DIRECTIONS.size()));
            this.date = LocalDate.of(2023, 2, RANDOM.nextInt(1, 29)).toString();
        }

        @Override
        public String toString() {
            return String.format(
                    "{(stationId,%d);(city,\"%s\");(temp,%d);(rain,%.2f);(wind,%d);(direction,\"%s\");(date,%s)}",
                    stationId, city, temp, rain, wind, direction, date);
        }
    }

    static class Subscription {
        private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
        private static final List<String> OPERATORS = Arrays.asList("=", "<", ">", "<=", ">=");
        private static final int MIN_TEMP = -20;
        private static final int MAX_TEMP = 40;
        private static final int MIN_WIND = 0;
        private static final int MAX_WIND = 100;

        private final Map<String, String[]> conditions = new LinkedHashMap<>();

        public Subscription(Map<String, Integer> fieldFrequencies, int equalityRatio) {
            // Ensure city field meets exact frequency and equality ratio requirements
            if (shouldIncludeField("city", fieldFrequencies)) {
                String op = (RANDOM.nextInt(100) <= equalityRatio) ? "=" :
                        OPERATORS.get(RANDOM.nextInt(1, OPERATORS.size()));
                addCondition("city", op, CITIES.get(RANDOM.nextInt(CITIES.size())));
            }

            if (shouldIncludeField("temp", fieldFrequencies)) {
                addCondition("temp",
                        OPERATORS.get(RANDOM.nextInt(OPERATORS.size())),
                        String.valueOf(RANDOM.nextInt(MIN_TEMP, MAX_TEMP + 1)));
            }

            if (shouldIncludeField("wind", fieldFrequencies)) {
                addCondition("wind",
                        OPERATORS.get(RANDOM.nextInt(OPERATORS.size())),
                        String.valueOf(RANDOM.nextInt(MIN_WIND, MAX_WIND + 1)));
            }

            if (shouldIncludeField("date", fieldFrequencies)) {
                addCondition("date",
                        OPERATORS.get(RANDOM.nextInt(OPERATORS.size())),
                        LocalDate.of(2023, 2, RANDOM.nextInt(1, 29)).toString());
            }

            if (conditions.isEmpty()) {
                List<String> keys = new ArrayList<>(fieldFrequencies.keySet());
                addCondition(keys.get(RANDOM.nextInt(keys.size())),
                        OPERATORS.get(RANDOM.nextInt(OPERATORS.size())),
                        CITIES.get(RANDOM.nextInt(CITIES.size())));
            }
        }

        private boolean shouldIncludeField(String field, Map<String, Integer> fieldFrequencies) {
            return RANDOM.nextInt(100) < fieldFrequencies.getOrDefault(field, 0);
        }

        public void addCondition(String field, String operator, String value) {
            conditions.put(field, new String[]{operator, value});
        }

        @Override
        public String toString() {
            return conditions.entrySet().stream()
                    .map(entry -> String.format("(%s,%s,%s)", entry.getKey(), entry.getValue()[0], entry.getValue()[1]))
                    .collect(Collectors.joining(";", "{", "}"));
        }
    }
}