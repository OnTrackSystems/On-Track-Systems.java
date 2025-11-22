package com.ontrack;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class EtlS3 {

    // Configurações
    private static final String BUCKET_RAW = "s3-raw-ontracksystems";
    private static final String BUCKET_TRUSTED = "s3-trusted-ontracksystems";
    private static final Region REGIAO = Region.US_EAST_1;

    public static void main(String[] args) {
        S3Client s3 = S3Client.builder()
                .region(REGIAO)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        try {
            ZoneId fusoBrasil = ZoneId.of("America/Sao_Paulo");
            ZonedDateTime alvo = ZonedDateTime.now(fusoBrasil).minusHours(1);

            String caminhoData = String.format("ano=%d/mes=%02d/dia=%02d/hora=%02d/",
                    alvo.getYear(), alvo.getMonthValue(), alvo.getDayOfMonth(), alvo.getHour());

            System.out.println("-> Iniciando ETL para o período: " + caminhoData);

            List<String> garagens = listarGaragens(s3);

            for (String garagemPrefix : garagens) {
                processarGaragem(s3, garagemPrefix, caminhoData);
            }

            System.out.println("ETL Global finalizada com sucesso!");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            s3.close();
        }
    }

    private static void processarGaragem(S3Client s3, String garagemPrefix, String caminhoData) throws IOException, CsvException {
        String prefixoCompleto = garagemPrefix + caminhoData;
        System.out.println("\nVerificando: " + prefixoCompleto);

        List<S3Object> arquivosS3 = listarArquivosDoPrefixo(s3, prefixoCompleto);

        if (arquivosS3.isEmpty()) {
            System.out.println("   [!] Nenhum dado encontrado nesta hora para " + garagemPrefix);
            return;
        }

        Path tempDir = Paths.get("temp", garagemPrefix, caminhoData);
        Files.createDirectories(tempDir);

        List<Path> arquivosBaixados = new ArrayList<>();

        for (S3Object s3Obj : arquivosS3) {
            String nomeArquivo = Paths.get(s3Obj.key()).getFileName().toString();
            Path destinoLocal = tempDir.resolve(nomeArquivo);

            downloadDoS3(s3, s3Obj.key(), destinoLocal);
            arquivosBaixados.add(destinoLocal);
        }

        String nomeArquivoFinal = "consolidado_" + System.currentTimeMillis() + ".csv";
        Path pathArquivoFinal = tempDir.resolve(nomeArquivoFinal);

        boolean sucesso = consolidarETratar(arquivosBaixados, pathArquivoFinal);

        if (sucesso) {
            String chaveDestino = prefixoCompleto + nomeArquivoFinal;
            uploadParaS3(s3, BUCKET_TRUSTED, chaveDestino, pathArquivoFinal);
            System.out.println("   [OK] Processamento concluído para " + garagemPrefix);
        }
    }

    private static boolean consolidarETratar(List<Path> inputs, Path output) throws IOException, CsvException {
        Set<String> timestampsVistos = new HashSet<>();
        boolean cabeçalhoEscrito = false;

        try (CSVWriter writer = new CSVWriter(new FileWriter(output.toFile()))) {

            for (Path inputPath : inputs) {
                try (CSVReader reader = new CSVReader(new FileReader(inputPath.toFile()))) {
                    List<String[]> linhas = reader.readAll();

                    if (linhas.isEmpty()) continue;

                    int inicioLeitura = 0;
                    if (!cabeçalhoEscrito) {
                        writer.writeNext(linhas.get(0));
                        cabeçalhoEscrito = true;
                        inicioLeitura = 1;
                    } else {
                        inicioLeitura = 1;
                    }

                    for (int i = inicioLeitura; i < linhas.size(); i++) {
                        String[] l = linhas.get(i);

                        if (l.length < 6) continue;

                        String timestamp = l[0];

                        try {
                            double cpu = Double.parseDouble(l[2]);
                            double ramPercent = Double.parseDouble(l[4]);
                            double disco = Double.parseDouble(l[5]);

                            if (cpu < 0 || cpu > 100) continue;
                            if (ramPercent < 0 || ramPercent > 100) continue;
                            if (disco < 0) continue;

                            if (!timestampsVistos.contains(timestamp)) {
                                timestampsVistos.add(timestamp);
                                writer.writeNext(l);
                            }

                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }
        }
        return true;
    }

    private static List<String> listarGaragens(S3Client s3) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_RAW)
                .delimiter("/")
                .build();

        ListObjectsV2Response response = s3.listObjectsV2(request);
        return response.commonPrefixes().stream()
                .map(CommonPrefix::prefix)
                .collect(Collectors.toList());
    }

    private static List<S3Object> listarArquivosDoPrefixo(S3Client s3, String prefixo) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_RAW)
                .prefix(prefixo)
                .build();
        return s3.listObjectsV2(request).contents();
    }

    private static void downloadDoS3(S3Client s3, String key, Path destino) {
        try {
            // 1. Verifica se o arquivo já existe e deleta para evitar o erro FileAlreadyExistsException
            Files.deleteIfExists(destino);

            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(BUCKET_RAW)
                    .key(key)
                    .build();

            // 2. Realiza o download
            s3.getObject(getReq, destino);

        } catch (IOException e) {
            System.err.println("Erro ao preparar local para arquivo: " + destino);
            e.printStackTrace();
        }
    }

    private static void uploadParaS3(S3Client s3, String bucket, String key, Path localPath) {
        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3.putObject(putReq, RequestBody.fromFile(localPath));
        System.out.println("   -> Uploaded: " + key);
    }
}