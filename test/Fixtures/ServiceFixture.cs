using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Arcane.Framework.Services.Base;
using Microsoft.Data.SqlClient;
using Moq;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Metrics.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Tests.Fixtures;

public class ServiceFixture
{
    public ServiceFixture()
    {
        MockBlobStorageService = new Mock<IBlobStorageService>();
        MockMetricsService = new Mock<MetricsService>();
        MockHttpClient = new Mock<HttpClient>();
        MockKubeCluster = new Mock<IKubeCluster>();
        MockStreamConfigurationProvider = new Mock<IStreamConfigurationProvider>();

        TestDbConnectionString = OperatingSystem.IsWindows()
            ? @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true;Trust Server Certificate=true"
            : "Server=.,1433;UID=sa;PWD=tMIxN11yGZgMC;TrustServerCertificate=true";

        using (var sqlCon = new SqlConnection(TestDbConnectionString))
        {
            sqlCon.Open();
            var createDbCmd =
                new SqlCommand(
                    "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'arcane') BEGIN CREATE DATABASE arcane; alter database Arcane set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON); END;",
                    sqlCon);
            createDbCmd.ExecuteNonQuery();
            sqlCon.Close();
        }
    }

    public Mock<IBlobStorageService> MockBlobStorageService { get; }
    public Mock<MetricsService> MockMetricsService { get; }
    public Mock<HttpClient> MockHttpClient { get; }
    public Mock<IKubeCluster> MockKubeCluster { get; }

    public Mock<IStreamConfigurationProvider> MockStreamConfigurationProvider { get; }

    public string TestDbConnectionString { get; }


    public string PrepareSqlServerDatabase(string tableName)
    {
        using (var sqlCon = new SqlConnection(TestDbConnectionString))
        {
            sqlCon.Open();
            var createTableCmd =
                new SqlCommand(
                    $"use arcane; drop table if exists dbo.{tableName}; create table dbo.{tableName}(x int not null, y int)",
                    sqlCon);
            createTableCmd.ExecuteNonQuery();

            var createPKCmd =
                new SqlCommand(
                    $"use arcane; alter table dbo.{tableName} add constraint pk_{tableName} primary key(x); ", sqlCon);
            createPKCmd.ExecuteNonQuery();

            var enableCtCmd =
                new SqlCommand($"use arcane; alter table dbo.{tableName} enable change_tracking; ", sqlCon);
            enableCtCmd.ExecuteNonQuery();

            var fillRowsCmds = Enumerable.Range(0, 100).Select(ix =>
                new SqlCommand($"use arcane; insert into dbo.{tableName} values ({ix}, {ix + 1})", sqlCon));
            foreach (var cmd in fillRowsCmds)
            {
                cmd.ExecuteNonQuery();
            }

            Thread.Sleep(5000);
            var changeRowCmd = new SqlCommand($"use arcane; insert into dbo.{tableName} values (9999, 9999)", sqlCon);
            changeRowCmd.ExecuteNonQuery();

            return $"{TestDbConnectionString};Initial Catalog=arcane;";
        }
    }

    public void InsertChanges(string tableName)
    {
        using var sqlCon = new SqlConnection(TestDbConnectionString);
        sqlCon.Open();
        var changeRowCmd =
            new SqlCommand($"use arcane; insert into dbo.{tableName} values (99999, 99999)", sqlCon);
        changeRowCmd.ExecuteNonQuery();
    }

    public void DeleteChanges(string tableName)
    {
        using var sqlCon = new SqlConnection(TestDbConnectionString);
        sqlCon.Open();
        var changeRowCmd =
            new SqlCommand($"use arcane; insert into dbo.{tableName} values (88888, 88888)", sqlCon);
        changeRowCmd.ExecuteNonQuery();
        var changeRowCmd2 =
            new SqlCommand($"use arcane; delete from dbo.{tableName} where x=88888", sqlCon);
        changeRowCmd2.ExecuteNonQuery();
    }

    public void CreateTable(string connectionString, string tableName, Dictionary<string, string> columns,
        string pkColumnName)
    {
        using var sqlCon = new SqlConnection(connectionString);
        sqlCon.Open();
        var cols = string.Join(",", columns.Select(c => $"{c.Key} {c.Value}"));
        var createTableCmd =
            new SqlCommand($"use arcane; drop table if exists dbo.{tableName}; create table dbo.{tableName} ({cols})",
                sqlCon);
        createTableCmd.ExecuteNonQuery();

        var createPKCmd =
            new SqlCommand(
                $"use arcane; alter table dbo.{tableName} add constraint pk_{tableName} primary key([{pkColumnName}]); ",
                sqlCon);
        createPKCmd.ExecuteNonQuery();

        var enableCtCmd = new SqlCommand($"use arcane; alter table dbo.{tableName} enable change_tracking; ", sqlCon);
        enableCtCmd.ExecuteNonQuery();
    }

    public void DropTable(string connectionString, string tableName)
    {
        using var sqlCon = new SqlConnection(connectionString);
        sqlCon.Open();
        var dropTableCmd = new SqlCommand($"use arcane; drop table if exists dbo.{tableName};", sqlCon);
        dropTableCmd.ExecuteNonQuery();
    }
}
