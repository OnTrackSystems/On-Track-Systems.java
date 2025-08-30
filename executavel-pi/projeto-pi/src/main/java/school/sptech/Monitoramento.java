package school.sptech;

import java.util.Random;

public class Monitoramento {
    private float cpu;
    private float ram;
    private float disco;
    private float tempoIo;
    private int pacotesEnviados;
    private int pacotesRecebidos;
    private Random random;

    public Monitoramento() {
        random = new Random();
    }

    public void gerarValores() {
        cpu = gerarValorAleatorio();
        ram = gerarValorAleatorio();
        disco = gerarValorAleatorio();
        tempoIo = gerarValorTempoIo();
        pacotesEnviados = gerarValorPacotes();
        pacotesRecebidos = gerarValorPacotes();
    }

    private float gerarValorAleatorio() {
        return Math.round(random.nextFloat() * 1000) / 10.0f;
    }

    private float gerarValorTempoIo() {
        return Math.round(random.nextFloat() * 1000);
    }

    private int gerarValorPacotes() {
        return random.nextInt(5000);
    }

    public float getCpu() {
        return cpu;
    }

    public float getRam() {
        return ram;
    }

    public float getDisco() {
        return disco;
    }

    public float getTempoIo() {
        return tempoIo;
    }

    public int getPacotesEnviados() {
        return pacotesEnviados;
    }

    public int getPacotesRecebidos() {
        return pacotesRecebidos;
    }
}