package school.sptech;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Main {
    public static void main(String[] args) {
        Scanner leitor = new Scanner(System.in);
        Monitoramento monitor = new Monitoramento();
        List<String> alertas = new ArrayList<>();

        boolean executando = true;

        while (executando) {
            System.out.println("\n--- MENU DE MONITORAMENTO ---");
            System.out.println("1 - Iniciar captura");
            System.out.println("2 - Exibir alertas capturados");
            System.out.println("3 - Encerrar programa");
            System.out.print("Escolha: ");
            int opcao = leitor.nextInt();

            switch (opcao) {
                case 1:
                    System.out.println("\nIniciando captura (10 coletas, uma a cada 2 segundos)...\n");

                    long ultimaCaptura = System.currentTimeMillis();

                    int totalCapturas = 10;
                    int capturasRealizadas = 0;

                    while (capturasRealizadas < totalCapturas) {
                        long agora = System.currentTimeMillis();
                        if (agora - ultimaCaptura >= 2000) {
                            ultimaCaptura = agora;
                            capturasRealizadas++;

                            // Captura o horário exato para cada coleta
                            LocalDateTime dataHoraAtual = LocalDateTime.now();
                            DateTimeFormatter formatador = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
                            String dtHoraFormatada = dataHoraAtual.format(formatador);

                            monitor.gerarValores();

                            float cpu = monitor.getCpu();
                            float ram = monitor.getRam();
                            float disco = monitor.getDisco();
                            float tempoIo = monitor.getTempoIo();
                            int pacotesEnviados = monitor.getPacotesEnviados();
                            int pacotesRecebidos = monitor.getPacotesRecebidos();

                            // Exibe as informações de hardware
                            System.out.printf("Horário da Captura: %s%n", dtHoraFormatada);
                            System.out.printf("HARDWARE - CPU: %.1f%% | RAM: %.1f%% | DISCO: %.1f%%%n",
                                    cpu, ram, disco);

                            // Exibe as informações de rede
                            System.out.printf("REDE - Tempo de I/O: %.1f ms | Pacotes Enviados: %d | Pacotes Recebidos: %d%n\n",
                                    tempoIo, pacotesEnviados, pacotesRecebidos);

                            // Verificação e alerta para hardware
                            if (cpu > 90) {
                                String alerta = String.format("ALERTA: CPU acima de 90%% (%.1f%%) | Data Hora: %s%n", cpu, dtHoraFormatada);
                                System.out.println(alerta);
                                alertas.add(alerta);
                            }

                            if (ram > 85) {
                                String alerta = String.format("ALERTA: RAM acima de 85%% (%.1f%%) | Data Hora: %s%n", ram, dtHoraFormatada);
                                System.out.println(alerta);
                                alertas.add(alerta);
                            }

                            if (disco > 80) {
                                String alerta = String.format("ALERTA: DISCO acima de 80%% (%.1f%%) | Data Hora: %s%n", disco, dtHoraFormatada);
                                System.out.println(alerta);
                                alertas.add(alerta);
                            }

                            // Verificação e alerta para rede
                            if (tempoIo > 200) {
                                String alertaRede = String.format("ALERTA: Tempo de I/O acima de 200ms (%.1f ms) | Data Hora: %s%n", tempoIo, dtHoraFormatada);
                                System.out.println(alertaRede);
                                alertas.add(alertaRede);
                            }

                            if (pacotesEnviados > 4000) {
                                String alertaPacotes = String.format("ALERTA: Pacotes Enviados acima de 4000 (%d pacotes) | Data Hora: %s%n", pacotesEnviados, dtHoraFormatada);
                                System.out.println(alertaPacotes);
                                alertas.add(alertaPacotes);
                            }

                            if (pacotesRecebidos > 4000) {
                                String alertaPacotesRecebidos = String.format("ALERTA: Pacotes Recebidos acima de 4000 (%d pacotes) | Data Hora: %s%n", pacotesRecebidos, dtHoraFormatada);
                                System.out.println(alertaPacotesRecebidos);
                                alertas.add(alertaPacotesRecebidos);
                            }
                        }
                    }

                    System.out.println("Captura finalizada. Retornando ao menu...\n");
                    break;

                case 2:
                    System.out.println("\n--- ALERTAS CAPTURADOS ---");
                    if (alertas.isEmpty()) {
                        System.out.println("Nenhum alerta registrado.");
                    } else {
                        for (String alerta : alertas) {
                            System.out.println(alerta);
                        }
                    }
                    break;

                case 3:
                    executando = false;
                    System.out.println("Encerrando o programa...");
                    break;

                default:
                    System.out.println("Opção inválida.");
            }
        }

        leitor.close();
    }
}