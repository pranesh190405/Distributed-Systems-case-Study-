package com.distributed.gui;

import com.distributed.master.MasterServer;
import com.distributed.model.MetricSnapshot;
import com.distributed.model.NodeInfo;
import com.distributed.model.TaskType;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.Stage;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * JavaFX Dashboard for the Distributed Computing Master Node.
 * Provides real-time monitoring, experiment control, and comparison reporting.
 */
public class DashboardApp extends Application {

    private MasterServer masterServer;

    // UI Components
    private TableView<NodeInfo> nodeTable;
    private ComboBox<TaskType> taskSelector;
    private ComboBox<String> algorithmSelector;
    private Button runButton;
    private Button runAllButton;
    private TextArea logArea;
    private TextArea resultArea;
    private TableView<Map<String, String>> comparisonTable;
    private Label statusLabel;
    private ProgressBar progressBar;

    // Data
    private ObservableList<NodeInfo> nodeData = FXCollections.observableArrayList();
    private ObservableList<Map<String, String>> comparisonData = FXCollections.observableArrayList();
    private volatile boolean isRunning = false;

    @Override
    public void start(Stage primaryStage) {
        masterServer = new MasterServer();
        try {
            masterServer.loadConfig("config.properties");
        } catch (Exception e) {
            showError("Failed to load config: " + e.getMessage());
            return;
        }

        // Set up log callback
        masterServer.setLogCallback(msg -> Platform.runLater(() -> {
            logArea.appendText(msg + "\n");
            logArea.setScrollTop(Double.MAX_VALUE);
        }));

        // Initialize master
        masterServer.initialize();
        nodeData.addAll(masterServer.getWorkers());

        // Build UI
        BorderPane root = new BorderPane();
        root.setStyle("-fx-background-color: #1a1a2e;");

        // Title Bar
        root.setTop(createTitleBar());

        // Main Content
        SplitPane mainSplit = new SplitPane();
        mainSplit.setStyle("-fx-background-color: #1a1a2e;");

        // Left side: Controls + Node Status
        VBox leftPane = createLeftPanel();
        // Right side: Log + Results + Comparison
        VBox rightPane = createRightPanel();

        mainSplit.getItems().addAll(leftPane, rightPane);
        mainSplit.setDividerPositions(0.35);
        root.setCenter(mainSplit);

        // Status Bar
        root.setBottom(createStatusBar());

        Scene scene = new Scene(root, 1400, 900);
        scene.getStylesheets().add("data:text/css," + getCustomCSS());

        primaryStage.setTitle("Distributed Computing Dashboard — Load Balancer Comparison");
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(e -> {
            masterServer.shutdown();
            Platform.exit();
            System.exit(0);
        });
        primaryStage.show();

        // Initial refresh
        refreshNodeStatus();
    }

    private HBox createTitleBar() {
        HBox titleBar = new HBox();
        titleBar.setAlignment(Pos.CENTER_LEFT);
        titleBar.setPadding(new Insets(15, 20, 15, 20));
        titleBar.setStyle("-fx-background-color: linear-gradient(to right, #16213e, #0f3460);");

        Label title = new Label("⚡ Distributed Scientific Computing Engine");
        title.setFont(Font.font("System", FontWeight.BOLD, 22));
        title.setTextFill(Color.web("#e94560"));

        Label subtitle = new Label("  |  Load Balancing Algorithm Comparison");
        subtitle.setFont(Font.font("System", FontWeight.NORMAL, 16));
        subtitle.setTextFill(Color.web("#a8a8b3"));

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        statusLabel = new Label("● System Ready");
        statusLabel.setFont(Font.font("System", FontWeight.BOLD, 14));
        statusLabel.setTextFill(Color.web("#00ff88"));

        titleBar.getChildren().addAll(title, subtitle, spacer, statusLabel);
        return titleBar;
    }

    private VBox createLeftPanel() {
        VBox panel = new VBox(15);
        panel.setPadding(new Insets(15));
        panel.setStyle("-fx-background-color: #16213e;");

        // --- Experiment Controls ---
        Label controlsLabel = createSectionLabel("🎮 Experiment Controls");

        // Task Type Selector
        Label taskLabel = new Label("Task Type:");
        taskLabel.setTextFill(Color.web("#a8a8b3"));
        taskSelector = new ComboBox<>(FXCollections.observableArrayList(TaskType.values()));
        taskSelector.setValue(TaskType.MONTE_CARLO_PI);
        taskSelector.setMaxWidth(Double.MAX_VALUE);
        taskSelector.setStyle(comboStyle());

        // Algorithm Selector
        Label algoLabel = new Label("Load Balancing Algorithm:");
        algoLabel.setTextFill(Color.web("#a8a8b3"));
        algorithmSelector = new ComboBox<>(FXCollections.observableArrayList(
                masterServer.getBalancers().keySet()));
        algorithmSelector.setValue("Round Robin");
        algorithmSelector.setMaxWidth(Double.MAX_VALUE);
        algorithmSelector.setStyle(comboStyle());

        // Run Button
        runButton = new Button("▶  Run Experiment");
        runButton.setMaxWidth(Double.MAX_VALUE);
        runButton.setStyle(buttonStyle("#e94560"));
        runButton.setFont(Font.font("System", FontWeight.BOLD, 14));
        runButton.setOnAction(e -> runExperiment());

        // Run All Button
        runAllButton = new Button("▶▶  Run All 15 Experiments");
        runAllButton.setMaxWidth(Double.MAX_VALUE);
        runAllButton.setStyle(buttonStyle("#0f3460"));
        runAllButton.setFont(Font.font("System", FontWeight.BOLD, 13));
        runAllButton.setOnAction(e -> runAllExperiments());

        // Refresh Workers Button
        Button refreshBtn = new Button("🔄  Refresh Workers");
        refreshBtn.setMaxWidth(Double.MAX_VALUE);
        refreshBtn.setStyle(buttonStyle("#533483"));
        refreshBtn.setOnAction(e -> refreshNodeStatus());

        // Progress bar
        progressBar = new ProgressBar(0);
        progressBar.setMaxWidth(Double.MAX_VALUE);
        progressBar.setStyle("-fx-accent: #e94560;");

        VBox controlsBox = new VBox(8, controlsLabel, taskLabel, taskSelector,
                algoLabel, algorithmSelector, runButton, runAllButton, refreshBtn, progressBar);

        // --- Node Status ---
        Label nodesLabel = createSectionLabel("🖥  Worker Nodes");

        nodeTable = new TableView<>(nodeData);
        nodeTable.setStyle("-fx-background-color: #0f3460; -fx-text-fill: white;");
        nodeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        TableColumn<NodeInfo, String> idCol = new TableColumn<>("Worker");
        idCol.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getId()));

        TableColumn<NodeInfo, String> statusCol = new TableColumn<>("Status");
        statusCol.setCellValueFactory(p -> new SimpleStringProperty(
                p.getValue().isAlive() ? "🟢 Online" : "🔴 Offline"));

        TableColumn<NodeInfo, String> weightCol = new TableColumn<>("Weight");
        weightCol.setCellValueFactory(p -> new SimpleStringProperty(
                String.valueOf(p.getValue().getWeight())));

        TableColumn<NodeInfo, String> activeCol = new TableColumn<>("Active");
        activeCol.setCellValueFactory(p -> new SimpleStringProperty(
                String.valueOf(p.getValue().getActiveTasks())));

        TableColumn<NodeInfo, String> avgCol = new TableColumn<>("Avg (ms)");
        avgCol.setCellValueFactory(p -> new SimpleStringProperty(
                String.format("%.1f", p.getValue().getAvgResponseTime())));

        TableColumn<NodeInfo, String> completedCol = new TableColumn<>("Done");
        completedCol.setCellValueFactory(p -> new SimpleStringProperty(
                String.valueOf(p.getValue().getCompletedTasks())));

        nodeTable.getColumns().addAll(idCol, statusCol, weightCol, activeCol, avgCol, completedCol);
        nodeTable.setPrefHeight(200);

        panel.getChildren().addAll(controlsBox, new Separator(), nodesLabel, nodeTable);
        return panel;
    }

    private VBox createRightPanel() {
        VBox panel = new VBox(10);
        panel.setPadding(new Insets(15));
        panel.setStyle("-fx-background-color: #16213e;");

        // Tabs for Log / Result / Comparison
        TabPane tabPane = new TabPane();
        tabPane.setStyle("-fx-background-color: #0f3460;");

        // Log Tab
        Tab logTab = new Tab("📋 Live Log");
        logTab.setClosable(false);
        logArea = new TextArea();
        logArea.setEditable(false);
        logArea.setStyle("-fx-control-inner-background: #0a0a1a; -fx-text-fill: #00ff88; " +
                "-fx-font-family: 'Monospaced'; -fx-font-size: 12px;");
        logArea.setPrefRowCount(15);
        logTab.setContent(logArea);

        // Result Tab
        Tab resultTab = new Tab("📊 Results");
        resultTab.setClosable(false);
        resultArea = new TextArea();
        resultArea.setEditable(false);
        resultArea.setStyle("-fx-control-inner-background: #0a0a1a; -fx-text-fill: #f0e68c; " +
                "-fx-font-family: 'Monospaced'; -fx-font-size: 13px;");
        resultTab.setContent(resultArea);

        // Comparison Tab
        Tab compTab = new Tab("📈 Comparison Table");
        compTab.setClosable(false);
        comparisonTable = createComparisonTable();
        VBox compBox = new VBox(10);
        compBox.setPadding(new Insets(10));

        Button exportBtn = new Button("📋 Copy to Clipboard");
        exportBtn.setStyle(buttonStyle("#533483"));
        exportBtn.setOnAction(e -> exportComparison());

        compBox.getChildren().addAll(comparisonTable, exportBtn);
        compTab.setContent(compBox);

        tabPane.getTabs().addAll(logTab, resultTab, compTab);
        VBox.setVgrow(tabPane, Priority.ALWAYS);

        panel.getChildren().add(tabPane);
        return panel;
    }

    private TableView<Map<String, String>> createComparisonTable() {
        TableView<Map<String, String>> table = new TableView<>(comparisonData);
        table.setStyle("-fx-background-color: #0f3460;");
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        String[] columns = {"Algorithm", "Task Type", "Total Time (ms)", "Throughput (tasks/s)",
                "Load Std Dev", "Max CPU %", "Result Summary"};

        for (String col : columns) {
            TableColumn<Map<String, String>, String> tc = new TableColumn<>(col);
            tc.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getOrDefault(col, "")));
            if (col.equals("Result Summary")) {
                tc.setPrefWidth(250);
            }
            table.getColumns().add(tc);
        }

        return table;
    }

    private HBox createStatusBar() {
        HBox bar = new HBox(10);
        bar.setAlignment(Pos.CENTER_LEFT);
        bar.setPadding(new Insets(8, 15, 8, 15));
        bar.setStyle("-fx-background-color: #0a0a1a;");

        Label info = new Label(String.format("Workers: %d  |  Matrix: %d×%d  |  MC Samples: %,d  |  Primes: %d  |  Chunks: %d",
                masterServer.getWorkers().size(), masterServer.getMatrixSize(), masterServer.getMatrixSize(),
                masterServer.getMonteCarloSamples(), masterServer.getPrimeCount(), masterServer.getChunksCount()));
        info.setTextFill(Color.web("#666680"));
        info.setFont(Font.font("System", 11));

        bar.getChildren().add(info);
        return bar;
    }

    // --- Actions ---

    private void runExperiment() {
        if (isRunning) return;
        isRunning = true;

        TaskType taskType = taskSelector.getValue();
        String algorithm = algorithmSelector.getValue();

        runButton.setDisable(true);
        runAllButton.setDisable(true);
        statusLabel.setText("⏳ Running...");
        statusLabel.setTextFill(Color.web("#f0e68c"));
        progressBar.setProgress(-1); // indeterminate

        CompletableFuture.supplyAsync(() -> {
            try {
                return masterServer.runExperiment(taskType, algorithm);
            } catch (Exception e) {
                Platform.runLater(() -> {
                    logArea.appendText("ERROR: " + e.getMessage() + "\n");
                });
                return null;
            }
        }).thenAccept(snapshot -> Platform.runLater(() -> {
            if (snapshot != null) {
                addToComparison(snapshot);
                resultArea.setText(snapshot.getResultSummary());
            }
            runButton.setDisable(false);
            runAllButton.setDisable(false);
            statusLabel.setText("● Experiment Complete");
            statusLabel.setTextFill(Color.web("#00ff88"));
            progressBar.setProgress(1);
            isRunning = false;
            refreshNodeStatus();
        }));
    }

    private void runAllExperiments() {
        if (isRunning) return;
        isRunning = true;

        runButton.setDisable(true);
        runAllButton.setDisable(true);
        statusLabel.setText("⏳ Running All 15 Experiments...");
        statusLabel.setTextFill(Color.web("#f0e68c"));
        progressBar.setProgress(0);

        comparisonData.clear();

        CompletableFuture.runAsync(() -> {
            TaskType[] tasks = TaskType.values();
            String[] algorithms = masterServer.getBalancers().keySet().toArray(new String[0]);
            int total = tasks.length * algorithms.length;
            int count = 0;

            for (TaskType task : tasks) {
                for (String algo : algorithms) {
                    count++;
                    int finalCount = count;
                    Platform.runLater(() -> {
                        statusLabel.setText(String.format("⏳ Experiment %d/%d: %s × %s",
                                finalCount, total, task.getDisplayName(), algo));
                        progressBar.setProgress((double) finalCount / total);
                    });

                    try {
                        MetricSnapshot snapshot = masterServer.runExperiment(task, algo);
                        Platform.runLater(() -> addToComparison(snapshot));

                        // Small pause between experiments
                        Thread.sleep(500);
                    } catch (Exception e) {
                        Platform.runLater(() ->
                                logArea.appendText("ERROR in " + task + " × " + algo + ": " + e.getMessage() + "\n"));
                    }
                }
            }

            Platform.runLater(() -> {
                runButton.setDisable(false);
                runAllButton.setDisable(false);
                statusLabel.setText("● All 15 Experiments Complete!");
                statusLabel.setTextFill(Color.web("#00ff88"));
                progressBar.setProgress(1);
                isRunning = false;
                refreshNodeStatus();
                resultArea.setText(buildComparisonReport());
            });
        });
    }

    private void addToComparison(MetricSnapshot s) {
        Map<String, String> row = new LinkedHashMap<>();
        row.put("Algorithm", s.getAlgorithmName());
        row.put("Task Type", s.getTaskType().getDisplayName());
        row.put("Total Time (ms)", String.valueOf(s.getTotalTimeMs()));
        row.put("Throughput (tasks/s)", String.format("%.2f", s.getThroughput()));
        row.put("Load Std Dev", String.format("%.3f", s.getLoadStdDev()));
        row.put("Max CPU %", String.format("%.1f", s.getMaxNodeUtilization()));

        // Truncate result summary for table
        String summary = s.getResultSummary();
        if (summary != null && summary.contains("\n")) {
            summary = summary.split("\n")[0];
        }
        if (summary != null && summary.length() > 60) {
            summary = summary.substring(0, 57) + "...";
        }
        row.put("Result Summary", summary != null ? summary : "N/A");

        comparisonData.add(row);
    }

    private String buildComparisonReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("╔══════════════════════════════════════════════════════════════╗\n");
        sb.append("║        COMPARATIVE ANALYSIS — ALL 15 EXPERIMENTS           ║\n");
        sb.append("╚══════════════════════════════════════════════════════════════╝\n\n");

        sb.append(String.format("%-22s %-28s %10s %12s %10s%n",
                "Algorithm", "Task Type", "Time(ms)", "Throughput", "Load Dev"));
        sb.append("─".repeat(85)).append("\n");

        for (Map<String, String> row : comparisonData) {
            sb.append(String.format("%-22s %-28s %10s %12s %10s%n",
                    row.get("Algorithm"),
                    row.get("Task Type"),
                    row.get("Total Time (ms)"),
                    row.get("Throughput (tasks/s)"),
                    row.get("Load Std Dev")));
        }

        sb.append("\n").append("─".repeat(85)).append("\n");
        sb.append("Report generated at: ").append(new Date()).append("\n");

        return sb.toString();
    }

    private void refreshNodeStatus() {
        CompletableFuture.runAsync(() -> {
            masterServer.getMetricsCollector().checkAllWorkers();
            Platform.runLater(() -> {
                nodeTable.refresh();
                long alive = nodeData.stream().filter(NodeInfo::isAlive).count();
                logArea.appendText(String.format("[Refresh] %d/%d workers online%n", alive, nodeData.size()));
            });
        });
    }

    private void exportComparison() {
        String report = buildComparisonReport();
        javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        javafx.scene.input.ClipboardContent content = new javafx.scene.input.ClipboardContent();
        content.putString(report);
        clipboard.setContent(content);
        logArea.appendText("[Export] Comparison report copied to clipboard.\n");
    }

    private void showError(String msg) {
        Alert alert = new Alert(Alert.AlertType.ERROR, msg, ButtonType.OK);
        alert.setTitle("Error");
        alert.showAndWait();
    }

    // --- Styles ---

    private Label createSectionLabel(String text) {
        Label label = new Label(text);
        label.setFont(Font.font("System", FontWeight.BOLD, 16));
        label.setTextFill(Color.web("#e94560"));
        return label;
    }

    private String comboStyle() {
        return "-fx-background-color: #0f3460; -fx-text-fill: white; " +
                "-fx-font-size: 13px; -fx-padding: 8; -fx-border-color: #533483; -fx-border-radius: 4;";
    }

    private String buttonStyle(String bgColor) {
        return String.format("-fx-background-color: %s; -fx-text-fill: white; " +
                "-fx-font-size: 13px; -fx-padding: 10 20; -fx-background-radius: 6; " +
                "-fx-cursor: hand;", bgColor);
    }

    private String getCustomCSS() {
        return String.join("",
                ".table-view { -fx-background-color: %230f3460; -fx-table-cell-border-color: %23333355; }",
                ".table-view .column-header { -fx-background-color: %2316213e; }",
                ".table-view .column-header .label { -fx-text-fill: %23e94560; -fx-font-weight: bold; -fx-font-size: 12px; }",
                ".table-view .table-row-cell { -fx-background-color: %230f3460; }",
                ".table-view .table-row-cell:odd { -fx-background-color: %2312254a; }",
                ".table-view .table-cell { -fx-text-fill: %23c8c8d4; -fx-font-size: 12px; }",
                ".tab-pane .tab-header-area { -fx-background-color: %230a0a1a; }",
                ".tab-pane .tab { -fx-background-color: %2316213e; }",
                ".tab-pane .tab:selected { -fx-background-color: %23e94560; }",
                ".tab-pane .tab .tab-label { -fx-text-fill: white; }",
                ".tab-pane .tab-content-area { -fx-background-color: %2316213e; }",
                ".split-pane-divider { -fx-background-color: %23333355; }",
                ".separator .line { -fx-border-color: %23333355; }",
                ".combo-box .list-cell { -fx-text-fill: white; -fx-background-color: %230f3460; }",
                ".combo-box-popup .list-view { -fx-background-color: %230f3460; }",
                ".combo-box-popup .list-cell { -fx-text-fill: white; -fx-background-color: %230f3460; }",
                ".combo-box-popup .list-cell:hover { -fx-background-color: %23e94560; }",
                ".progress-bar .track { -fx-background-color: %230a0a1a; }",
                ".progress-bar .bar { -fx-background-color: %23e94560; }",
                ".scroll-bar { -fx-background-color: %230f3460; }",
                ".scroll-bar .thumb { -fx-background-color: %23533483; }"
        );
    }

    public static void main(String[] args) {
        launch(args);
    }
}
