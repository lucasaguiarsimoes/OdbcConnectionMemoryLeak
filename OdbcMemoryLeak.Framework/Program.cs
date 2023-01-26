using System.Collections.Generic;
using System;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;
using System.Threading.Tasks;

namespace OdbcMemoryLeak
{
    public static class Program
    {
        private static string _connectionString = "DRIVER={Intersystems ODBC35};SERVER=LOCALHOST;PORT=56773;DATABASE=INTERF;PROTOCOL=TCP;UID=_SYSTEM;PWD=SYS";

        public static void Main(string[] args)
        {
            List<Task> tasks = new List<Task>();

            // Create 10 tasks to run forever to test memory leak
            for (int index = 1; index <= 10; index++)
            {
                int currentIndex = index;
                tasks.Add(Task.Run(() => Execute(currentIndex)));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private static void Execute(int index)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            OdbcConnection connection = new OdbcConnection(_connectionString);

            connection.Open();

            ToConsole(stopwatch, $"CONNECTION {index} IS OPENED!");

            // Change this boolean to false to execute the same procedure WITHOUT memory leak
            bool withMemoryLeak = true;

            try
            {
                while (true)
                {
                    if (withMemoryLeak)
                    {
                        // Reproduces memory leak in the following scenario: Stored procedure executed with 'CALL' and OdbcConnection
                        WithMemoryLeak(connection);
                    }
                    else
                    {
                        // Method to simulate the execution of the same stored procedure WITHOUT memory leak
                        NoMemoryLeak(connection);
                    }
                }
            }
            catch (Exception exc)
            {
                ToConsole(stopwatch, $"ERROR IN CONNECTION {index} - {exc}");
            }
            finally
            {
                connection.Close();
            }
        }

        private static void WithMemoryLeak(OdbcConnection connection)
        {
            // Aciona uma procedure no Caché por conta do Pool de conexões. Salva variáveis locais do Caché para não serem apagadas
            string commandText = "CALL MCI.Example_IsAlive()";

            using (var objComando = GetCommand(connection, commandText))
            {
                objComando.CommandType = CommandType.StoredProcedure;
                objComando.Prepare();
                objComando.ExecuteNonQuery();
            }
        }

        private static void NoMemoryLeak(OdbcConnection connection)
        {
            // Using SELECT instead of CALL (when possible) avoids memory leak
            string commandText = "SELECT MCI.Example_IsAlive()";

            using (var objComando = GetCommand(connection, commandText))
            {
                objComando.CommandType = CommandType.StoredProcedure;
                objComando.Prepare();
                objComando.ExecuteNonQuery();
            }
        }

        private static DbCommand GetCommand(OdbcConnection connection, string sTexto)
        {
            return new OdbcCommand(sTexto, (OdbcConnection)connection, null);
        }

        private static void ToConsole(Stopwatch stopwatch, string text)
        {
            Console.WriteLine($"[{stopwatch.Elapsed.ToString(@"hh':'mm':'ss'.'fff")}] {text}");
        }
    }
}



