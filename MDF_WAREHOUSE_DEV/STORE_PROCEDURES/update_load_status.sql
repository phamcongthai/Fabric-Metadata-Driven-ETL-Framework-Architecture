CREATE OR ALTER   PROCEDURE [mdf_platform_orchestration].[update_load_status]
    @table_id               VARCHAR(200),
    @layer                  VARCHAR(200),
    @pipeline_id            VARCHAR(200),
    @pipeline_name          VARCHAR(200),
    @pipeline_run_id        VARCHAR(200),
    @original_run_id        VARCHAR(200),
    @pipeline_trigger_id    VARCHAR(200),
    @pipeline_trigger_time  VARCHAR(200),
    @pipeline_trigger_type  VARCHAR(200),
    @last_ingest_partition  VARCHAR(200)
AS
BEGIN
    DECLARE @table_name  VARCHAR(500)
    DECLARE @data_subject VARCHAR(200)

    SELECT @table_name = tablename, @data_subject = datasubject
    FROM mdf_platform_orchestration.elt_table_config
    WHERE table_id = @table_id

    BEGIN TRY
        IF @layer = 'src2brz'
            UPDATE mdf_platform_orchestration.elt_table_config
            SET ingest_partition = @last_ingest_partition
            WHERE table_id = @table_id

        ELSE IF @layer = 'brz2sil'
            UPDATE mdf_platform_orchestration.elt_table_config
            SET ingest_partition = @last_ingest_partition,
                last_loaded_dt = TRY_CAST(
                    LEFT(@last_ingest_partition, 10) + ' ' +
                    RIGHT(@last_ingest_partition, 2) + ':00:00'
                    AS DATETIME2)
            WHERE table_id = @table_id
        ELSE
            THROW 50001, 'Invalid layer value', 1

        EXEC mdf_platform_orchestration.usp_insert_load_history
            @data_subject    = @data_subject,
            @table_name      = @table_name,
            @layer           = @layer,
            @pipeline_run_id = @pipeline_run_id,
            @original_run_id = @original_run_id

        EXEC mdf_platform_orchestration.usp_resolve_error
            @table_name    = @table_name,
            @pipeline_name = @pipeline_name,
            @layer         = @layer

    END TRY

    BEGIN CATCH
        IF ERROR_NUMBER() = 24556
        BEGIN
            IF @layer = 'src2brz'
                UPDATE mdf_platform_orchestration.elt_table_config
                SET ingest_partition = @last_ingest_partition
                WHERE table_id = @table_id
            ELSE IF @layer = 'brz2sil'
                UPDATE mdf_platform_orchestration.elt_table_config
                SET ingest_partition = @last_ingest_partition,
                    last_loaded_dt = TRY_CAST(
                        LEFT(@last_ingest_partition, 10) + ' ' +
                        RIGHT(@last_ingest_partition, 2) + ':00:00'
                        AS DATETIME2)
                WHERE table_id = @table_id

            EXEC mdf_platform_orchestration.usp_insert_load_history
                @data_subject    = @data_subject,
                @table_name      = @table_name,
                @layer           = @layer,
                @pipeline_run_id = @pipeline_run_id,
                @original_run_id = @original_run_id

            EXEC mdf_platform_orchestration.usp_resolve_error
                @table_name    = @table_name,
                @pipeline_name = @pipeline_name,
                @layer         = @layer
        END
        ELSE
            THROW
    END CATCH
END