package com.ontrack;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
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

public class EtlS3 implements RequestHandler<Object, String> {

    private static final String BUCKET_RAW = System.getenv().getOrDefault("BUCKET_RAW", "s3-raw-ontracksystems");
    private static final String BUCKET_TRUSTED = System.getenv().getOrDefault("BUCKET_TRUSTED", "s3-trusted-ontracksystems");
    private static final Region REGIAO = Region.US_EAST_1;

    private static final S3Client s3 = S3Client.builder()
            .region(REGIAO)
            .build();

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Iniciando ETL Lambda...");

        try {
            ZoneId fusoBrasil = ZoneId.of("America/Sao_Paulo");
            ZonedDateTime alvo = ZonedDateTime.now(fusoBrasil).minusHours(1);

            context.getLogger().log("-> Processando referência: " + alvo);

            List<String> garagens = listarGaragens(s3);

            for (String garagemPrefix : garagens) {
                processarGaragem(s3, garagemPrefix, alvo, context);
            }

            return "Sucesso! Dados processados.";

        } catch (Exception e) {
            context.getLogger().log("ERRO FATAL: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void processarGaragem(S3Client s3, String garagemPrefix, ZonedDateTime alvo, Context context) throws IOException, CsvException {

        String caminhoLeituraRaw = String.format("ano=%d/mes=%02d/dia=%02d/hora=%02d/",
                alvo.getYear(), alvo.getMonthValue(), alvo.getDayOfMonth(), alvo.getHour());

        String prefixoCompletoRaw = garagemPrefix + caminhoLeituraRaw;

        List<S3Object> arquivosS3 = listarArquivosDoPrefixo(s3, prefixoCompletoRaw);

        if (arquivosS3.isEmpty()) {
            System.out.println("   [!] " + garagemPrefix + ": Nada na hora " + alvo.getHour());
            return;
        }

        Path tempDir = Paths.get("/tmp", garagemPrefix, caminhoLeituraRaw);
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }

        List<Path> arquivosBaixados = new ArrayList<>();

        for (S3Object s3Obj : arquivosS3) {
            String nomeArquivo = Paths.get(s3Obj.key()).getFileName().toString();
            Path destinoLocal = tempDir.resolve(nomeArquivo);

            downloadDoS3(s3, s3Obj.key(), destinoLocal);
            arquivosBaixados.add(destinoLocal);
        }

        String caminhoEscritaTrusted = String.format("ano=%d/mes=%02d/dia=%02d/",
                alvo.getYear(), alvo.getMonthValue(), alvo.getDayOfMonth());

        String nomeArquivoFinal = String.format("consolidado_%02d.csv", alvo.getHour());

        Path pathArquivoFinal = tempDir.resolve(nomeArquivoFinal);

        boolean sucesso = consolidarETratar(arquivosBaixados, pathArquivoFinal);

        if (sucesso) {
            String chaveDestino = garagemPrefix + caminhoEscritaTrusted + nomeArquivoFinal;

            uploadParaS3(s3, BUCKET_TRUSTED, chaveDestino, pathArquivoFinal);

            context.getLogger().log("   [OK] Gerado: " + chaveDestino);
        }

        limparTemp(tempDir);
    }

    private boolean consolidarETratar(List<Path> inputs, Path output) throws IOException, CsvException {
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
                        } catch (NumberFormatException e) {}
                    }
                }
            }
        }
        return true;
    }

    private List<String> listarGaragens(S3Client s3) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_RAW)
                .delimiter("/")
                .build();
        return s3.listObjectsV2(request).commonPrefixes().stream()
                .map(CommonPrefix::prefix)
                .collect(Collectors.toList());
    }

    private List<S3Object> listarArquivosDoPrefixo(S3Client s3, String prefixo) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_RAW)
                .prefix(prefixo)
                .build();
        return s3.listObjectsV2(request).contents();
    }

    private void downloadDoS3(S3Client s3, String key, Path destino) {
        try {
            Files.deleteIfExists(destino);

            GetObjectRequest getReq = GetObjectRequest.builder().bucket(BUCKET_RAW).key(key).build();
            s3.getObject(getReq, destino);
        } catch (IOException e) {
            System.err.println("Erro download: " + e.getMessage());
        }
    }

    private void uploadParaS3(S3Client s3, String bucket, String key, Path localPath) {
        PutObjectRequest putReq = PutObjectRequest.builder().bucket(bucket).key(key).build();
        s3.putObject(putReq, RequestBody.fromFile(localPath));
    }

    private void limparTemp(Path path) {
        try {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        } catch (Exception e) {}
    }
}