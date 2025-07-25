using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

public class DataObject
{
    public string Name { get; set; }
    public int ReadTimestamp { get; set; }
    public int WriteTimestamp { get; set; }

    public DataObject(string name)
    {
        Name = name;
        Reset();
    }

    // Reinicia os timestamps para um novo escalonamento
    public void Reset()
    {
        ReadTimestamp = 0;
        WriteTimestamp = 0;
    }
}

// Classe principal que executa o algoritmo de escalonamento
public class TimestampScheduler
{
    // Estruturas para armazenar os objetos de dados e os timestamps das transações
    private static Dictionary<string, DataObject> _dataObjects;
    private static Dictionary<int, int> _transactionTimestamps;

    public static void Main(string[] args)
    {
        string inputFile = "in.txt";
        string outputFile = "out.txt";

        // Verifica se o arquivo de entrada existe
        if (!File.Exists(inputFile))
        {
            Console.WriteLine($"Erro: Arquivo de entrada '{inputFile}' não encontrado.");
            // Cria um arquivo de exemplo se não existir
            arquivoExemplo(inputFile);
            return;
        }

        // Lê todas as linhas do arquivo de entrada
        var lines = File.ReadAllLines(inputFile);
        var outputLines = new List<string>();

        try
        {
            //  Interpretador dos Objetos de Dados
            var dataNames = lines[0].Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToList();
            _dataObjects = dataNames.ToDictionary(name => name, name => new DataObject(name));

            // Limpa arquivos de log antigos dos objetos de dados
            foreach (var dataName in dataNames)
            {
                File.Delete($"{dataName}.txt");
            }

            // Armazena as Transações (nomes)
            var transactionNames = lines[1].Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                                           .Select(s => s.Trim()).ToList();

            //  armazena os Timestamps
            var timestamps = lines[2].Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(int.Parse).ToList();
            _transactionTimestamps = new Dictionary<int, int>();
            for (int i = 0; i < transactionNames.Count; i++)
            {
                // Extrai o número da transação (ex: de "t1" para 1)
                int transactionId = int.Parse(transactionNames[i].Substring(1));
                _transactionTimestamps[transactionId] = timestamps[i];
            }

            // Processamento de cada Escalonamento
            for (int i = 3; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;

                // Reseta os timestamps dos objetos de dados para o novo escalonamento
                foreach (var dataObject in _dataObjects.Values)
                {
                    dataObject.Reset();
                }

                // Processa o escalonamento atual e obtém o resultado
                string result = ProcessSchedule(lines[i]);
                outputLines.Add(result);
            }

            // Escreve o resultado final no arquivo de saída
            File.WriteAllLines(outputFile, outputLines);
            Console.WriteLine($"Processamento concluído. Resultados salvos em '{outputFile}'.");
            Console.WriteLine("Arquivos de log para cada objeto de dado também foram gerados.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ocorreu um erro ao processar o arquivo: {ex.Message}");
        }
    }

    // Processa uma única linha de escalonamento
    private static string ProcessSchedule(string scheduleLine)
    {
        var parts = scheduleLine.Split('-', 2);
        string scheduleId = parts[0].Trim();
        string operationsStr = parts[1].Trim();

        // Expressão regular para extrair operações como r1(A), w2(B), c
        var operationRegex = new Regex(@"(r|w)(\d+)\((\w+)\)|(c)");
        var matches = operationRegex.Matches(operationsStr);

        int moment = 0;

        foreach (Match match in matches)
        {
            // Verifica se é uma operação de commit 'c'
            if (match.Groups[4].Value == "c")
            {
                moment++;
                continue; // Pula para a próxima operação
            }

            // Extrai detalhes da operação (tipo, ID da transação, objeto de dado)
            char operationType = match.Groups[1].Value[0];
            int transactionId = int.Parse(match.Groups[2].Value);
            string dataObjectName = match.Groups[3].Value;

            int transactionTimestamp = _transactionTimestamps[transactionId];
            DataObject dataObject = _dataObjects[dataObjectName];

            bool rollback = false;

            // algoritmo de timestamp
            if (operationType == 'r') // Operação de Leitura (Read) 
            {
                // Regra de Leitura: TS(Ti) < WTS(X) -> ROLLBACK
                if (transactionTimestamp < dataObject.WriteTimestamp)
                {
                    rollback = true;
                }
                else
                {
                    // Atualiza o ReadTimestamp do objeto
                    dataObject.ReadTimestamp = Math.Max(dataObject.ReadTimestamp, transactionTimestamp);
                    LogOperation(scheduleId, "read", moment, dataObjectName);
                }
            }
            else if (operationType == 'w') // Escrita (Write)
            {
                // Regra de Escrita: TS(Ti) < RTS(X) ou TS(Ti) < WTS(X) -> ROLLBACK
                if (transactionTimestamp < dataObject.ReadTimestamp || transactionTimestamp < dataObject.WriteTimestamp)
                {
                    rollback = true;
                }
                else
                {
                    // Atualiza o WriteTimestamp do objeto
                    dataObject.WriteTimestamp = transactionTimestamp;
                    LogOperation(scheduleId, "write", moment, dataObjectName);
                }
            }

            // depois de detectado, registra o rollback
            if (rollback)
            {
                return $"{scheduleId}-ROLLBACK-{moment}";
            }

            moment++;
        }

        // Se nenhum conflito ocorreu em todo o escalonamento, ele é serializável
        return $"{scheduleId}-OK";
    }

    // Função para registrar a operação no arquivo de log do objeto de dado
    private static void LogOperation(string scheduleId, string operation, int moment, string dataObjectName)
    {
        string logFileName = $"{dataObjectName}.txt";
        string logEntry = $"{scheduleId},{operation},{moment}";
        File.AppendAllText(logFileName, logEntry + Environment.NewLine);
    }

    // Metodo para criar um arquivo txt de exemplo
    private static void arquivoExemplo(string filePath)
    {
        Console.WriteLine($"Criando arquivo de exemplo '{filePath}'...");
        string[] content =
        {
            "A, B, C, D;",
            "t1, t2, t3, t4;",
            "8, 9, 1, 4;",
            "E_1-r1(A) r4(A) r3(A) r3(B) r2(A) c",
            "E_2-r1(A) c w4(A) r2(A) r3(C) c",
            "E_3-w4(B) r1(B) r2(B) c r4(A) r3(A) r3(D) w3(D) r2(D) r2(B) c",
            "E_4-w4(B) r1(B) r2(B) c r4(A) r3(A) r3(D) w3(D) r4(D) w4(D) r2(C) w1(D) w3(D) c r3(C) r3(B) r2(A) c",
            "E_5-w4(B) r1(B) r2(B) c r4(A) r3(A) r3(D) w3(D) r4(D) w4(D) r2(C) w1(D) c w3(D) r3(C) r3(B) r2(A) c",
            "E_6-r1(A) r2(A) w2(B) w3(C) c w3(B) w4(A) w4(B) c",
            "E_7-w1(A) r2(B) r1(B) w2(B) r1(A) c w3(B) w4(A) w2(B) c",
            "E_8-w1(A) r2(B) r1(B) w2(B) r1(A) w3(B) w4(A) w2(B) c",
            "E_9-w1(A) r2(B) r1(B) r1(A) w3(B) w4(A) w2(B) c"
        };
        File.WriteAllLines(filePath, content);
        Console.WriteLine("Arquivo de exemplo criado. Por favor, execute o programa novamente.");
    }
}
