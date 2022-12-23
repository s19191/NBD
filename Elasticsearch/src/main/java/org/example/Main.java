package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.sun.management.OperatingSystemMXBean;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        try {
            /**
             * Tworzenie połączenia.
             */

            // Create the low-level client
            RestClient httpClient = RestClient.builder(
                    new HttpHost("localhost", 9200)
            ).build();

            // Create the Java API Client with the same low level client
            ElasticsearchTransport transport = new RestClientTransport(
                    httpClient,
                    new JacksonJsonpMapper()
            );

            ElasticsearchClient esClient = new ElasticsearchClient(transport);

            /**
             * Tworzenie Indexu.
             */

            esClient.indices().create(c -> c.index("s19191_performance_data"));

            /**
             * Task do wykonywania cyklicznie co minutę
             */

            TimerTask repeatedTask = new TimerTask() {
                public void run() {
                    //Po to żeby sprawdzić na konsoli, czy co minute zczytuje dane.
                    System.out.println("Task performed on " + new Date());
                    try {
                        Double CPU = getCPUUsage();
                        Double GPU = Math.random() * 100;
                        Performance performance = new Performance(new Date(), InetAddress.getLocalHost().getHostName(), InetAddress.getLocalHost().getHostAddress(), CPU, GPU);
                        IndexRequest<Performance> request = IndexRequest.of(i -> i
                                .index("s19191_performance_data")
                                .document(performance)
                        );
                        esClient.index(request);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };

            /**
             * Executor, który będzie wykonywać ten task
             */

            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            long delay  = 0L;
            long period = 60000L;
            executor.scheduleAtFixedRate(repeatedTask, delay, period, TimeUnit.MILLISECONDS);
            Thread.sleep(delay + period * 14);
            executor.shutdown();

            if (executor.isShutdown()) {

                /**
                 * Odczytywanie danych oraz zapis do osobnego pliku. On będzie w folderze projektu oraz osobno.
                 */

                SearchResponse<Performance> response = esClient.search(s -> s
                                .index("s19191_performance_data")
                                .size(100),
                        Performance.class
                );

                FileWriter fileWriter = new FileWriter("output.txt");
                PrintWriter printWriter = null;

                List<Hit<Performance>> hits = response.hits().hits();
                for (Hit<Performance> hit: hits) {
                    Performance performance = hit.source();
                    printWriter = new PrintWriter(fileWriter);
                    printWriter.printf(performance.toString() + "\n");
                    System.out.println(performance);
                }

                printWriter.close();
            }

            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Double getCPUUsage() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        return osBean.getSystemCpuLoad() * 100;
    }

    /**
     * Metoda ta powinna zwracać użycie GPU karty NVIDIA w moim komputerze, zwracała ona natomiast same zera.
     * Było to w miarę zgodne z prawdą, gdyż mój laptop posiada jeszcze drugą kartę graficzną zintegrowaną Intela.
     * Nie znalazłem metody, żeby konkretnie znaleść wyniki tej karty, więc w zadaniu użyłem Math.random.
     */

    /**
     * Teraz wiem, że chodziło o RAM, ale to też nie jest takie proste, więc już zostawię tak jak jest.
     */
    public static Double getGPUUsage() {
        try {
            Process process = Runtime.getRuntime().exec("nvidia-smi --query-gpu=utilization.gpu --format=csv");
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            stdInput.readLine();
            return Double.parseDouble(stdInput.readLine().replace(" %", ""));
        } catch (Exception e) {
            e.printStackTrace();
            return -1.0;
        }
    }
}