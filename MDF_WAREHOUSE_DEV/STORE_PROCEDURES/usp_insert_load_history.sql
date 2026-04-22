CREATE OR ALTER   PROCEDURE [mdf_platform_orchestration].[usp_insert_load_history]
    @data_subject       VARCHAR(200),
    @table_name         VARCHAR(500),
    @layer              VARCHAR(200),
    @pipeline_run_id    VARCHAR(200),
    @original_run_id    VARCHAR(200),
    @record_count       INT = 0
AS
BEGIN
    DECLARE @next_id BIGINT

    BEGIN TRY
        SELECT @next_id = ISNULL(MAX(load_id), 0) + 1
        FROM mdf_platform_orchestration.load_history

        INSERT INTO mdf_platform_orchestration.load_history
        (
            load_id,
            data_subject,
            table_name,
            layer,
            pipeline_run_id,
            record_count,
            load_ts
        )
        VALUES
        (
            @next_id,
            @data_subject,
            @table_name,
            @layer,
            @original_run_id,
            @record_count,
            GETUTCDATE()
        )
    END TRY

    BEGIN CATCH
        IF ERROR_NUMBER() = 24556
        BEGIN
            SELECT @next_id = ISNULL(MAX(load_id), 0) + 1
            FROM mdf_platform_orchestration.load_history

            INSERT INTO mdf_platform_orchestration.load_history
            (
                load_id,
                data_subject,
                table_name,
                layer,
                pipeline_run_id,
                record_count,
                load_ts
            )
            VALUES
            (
                @next_id,
                @data_subject,
                @table_name,
                @layer,
                @original_run_id,
                @record_count,
                GETUTCDATE()
            )
        END
        ELSE
            THROW
    END CATCH
END