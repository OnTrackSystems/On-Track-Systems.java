package com.ontrack;

import com.opencsv.exceptions.CsvException;
// Software developer kit para conexão com o S3
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import com.opencsv.*;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class EtlS3 {
    public static void main(String[] args) {
        String bucketRaw = "raw-ontrack"; // bucket de origem
        String bucketProcessed = "trusted-ontrack"; // bucket destino
        String caminhoRaw = "coletaGeralOTS.csv"; // arquivo no S3
        String localRaw = "temp/coletaGeralOTS.csv"; // caminho local temporário
        String localProcessed = "temp/coletaTratado.csv"; // arquivo tratado

        Region regiao = Region.US_EAST_1; // Região do bucket
        // Criando o objeto "s3" que faz as chamadas para API do S3
        S3Client s3 = S3Client.builder()
                .region(regiao)
                .credentialsProvider(ProfileCredentialsProvider.create()) // Busca as credenciais p/ conexão
                .build();

        try {
            System.out.println("-> Baixando arquivo do bucket RAW");
            downloadDoS3(s3, bucketRaw, caminhoRaw, localRaw);

            System.out.println("-> Realizando tratamento do arquivo baixado");
            processarS3(localRaw, localProcessed);

            System.out.println("-> Enviando arquivo tratado para o bucket TRUSTED");
            uploadParaS3(s3, bucketProcessed, "coletaTratado.csv", localProcessed);

            System.out.println("ETL finalizada com sucesso!");

        } catch (Exception e) {
            System.out.println("Erro ao realizar operações no arquivo: " + e);
        } finally {
            s3.close();
        }
    }

    private static void downloadDoS3(S3Client s3, String bucket, String caminhoRaw, String localPath) throws IOException {
        Path caminho = Paths.get(localPath);
        Files.createDirectories(caminho.getParent());

        GetObjectRequest getReq = GetObjectRequest.builder() // requisão p/ baixar o arquivo do RAW
                .bucket(bucket) // nome do bucket
                .key(caminhoRaw) // nome do arquivo
                .build(); 

        s3.getObject(getReq, caminho);
    }

    private static void uploadParaS3(S3Client s3, String bucket, String key, String localPath) throws IOException {
        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        s3.putObject(putReq, Paths.get(localPath));

        Path caminhoRaw = Paths.get("temp/coletaGeralOTS.csv");
        Path caminhoTratado = Paths.get("temp/coletaTratado.csv");


        try {
            Files.delete(caminhoTratado);
            Files.delete(caminhoRaw);
            System.out.println("Arquivos locais excluidos após upload para o bucket Trusted");
        } catch (IOException e) {
            System.err.println("Falha ao excluir arquivo local tratado " + localPath + ": " + e.getMessage());
        }

    }

    private static void processarS3(String inputPath, String outputPath) throws IOException, CsvException {
        try ( // bloco try-with-resources garante que o reader e o writer sejam fechados mesmo se der erro
                CSVReader reader = new CSVReader(new FileReader(inputPath));
                CSVWriter writer = new CSVWriter(new FileWriter(outputPath))
        ) {
            List<String[]> linhas = reader.readAll();
            List<String> vistos = new ArrayList<>(); // timestamps já vistos

            // Cabeçalho original
            writer.writeNext(linhas.get(0));

            // Percorre as linhas e executa as validações
            for (int i = 1; i < linhas.size(); i++) {
                String[] l = linhas.get(i);
                String timestamp = l[0];

                try {
                    double cpu = Double.parseDouble(l[2]);
                    double ramPercent = Double.parseDouble(l[4]);
                    double disco = Double.parseDouble(l[5]);

                    // Filtros para evitar dados "impossíveis"
                    if (cpu < 0 || cpu > 100) continue;
                    if (ramPercent < 0 || ramPercent > 100) continue;
                    if (disco < 0) continue;

                    // Evita duplicados
                    if (!vistos.contains(timestamp)) {
                        vistos.add(timestamp);
                        writer.writeNext(l);
                    }

                } catch (Exception e) {
                }
            }
        }
    }
}
