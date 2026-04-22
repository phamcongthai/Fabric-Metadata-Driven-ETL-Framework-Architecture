CREATE OR ALTER   PROCEDURE [mdf_platform_orchestration].[usp_log_error]
    @database_name                  VARCHAR(200) = 'MDF_WAREHOUSE_DEV_001',
    @event_type                     VARCHAR(200),
    @schema_name                    VARCHAR(200) = 'mdf_platform_orchestration',
    @object_name                    VARCHAR(500),
    @object_type                    VARCHAR(200),
    @error_nbr                      INT = 0,
    @error_severity                 INT = 0,
    @error_state                    INT = 0,
    @message                        VARCHAR(MAX),
    @pipeline_name                  VARCHAR(500),
    @pipeline_run_id                VARCHAR(200),
    @pipeline_trigger_id            VARCHAR(200),
    @pipeline_trigger_type          VARCHAR(200),
    @pipeline_trigger_date_time_utc VARCHAR(200)
AS
BEGIN
    DECLARE @next_id BIGINT

    BEGIN TRY
        SELECT @next_id = ISNULL(MAX(log_id), 0) + 1
        FROM mdf_platform_orchestration.error_log

        INSERT INTO mdf_platform_orchestration.error_log
        (
            log_id,
            database_name,
            event_type,
            schema_name,
            object_name,
            object_type,
            error_nbr,
            error_severity,
            error_state,
            message,
            created_date_time,
            pipeline_name,
            pipeline_run_id,
            pipeline_trigger_name,
            pipeline_trigger_id,
            pipeline_trigger_type,
            pipeline_trigger_date_time_utc,
            active_flag
        )
        VALUES
        (
            @next_id,
            @database_name,
            @event_type,
            @schema_name,
            @object_name,
            @object_type,
            @error_nbr,
            @error_severity,
            @error_state,
            @message,
            GETUTCDATE(),
            @pipeline_name,
            @pipeline_run_id,
            @pipeline_name,
            @pipeline_trigger_id,
            @pipeline_trigger_type,
            TRY_CAST(@pipeline_trigger_date_time_utc AS DATETIME2),
            1
        )
    END TRY

    BEGIN CATCH
        IF ERROR_NUMBER() = 24556
        BEGIN
            SELECT @next_id = ISNULL(MAX(log_id), 0) + 1
            FROM mdf_platform_orchestration.error_log

            INSERT INTO mdf_platform_orchestration.error_log
            (
                log_id,
                database_name,
                event_type,
                schema_name,
                object_name,
                object_type,
                error_nbr,
                error_severity,
                error_state,
                message,
                created_date_time,
                pipeline_name,
                pipeline_run_id,
                pipeline_trigger_name,
                pipeline_trigger_id,
                pipeline_trigger_type,
                pipeline_trigger_date_time_utc,
                active_flag
            )
            VALUES
            (
                @next_id,
                @database_name,
                @event_type,
                @schema_name,
                @object_name,
                @object_type,
                @error_nbr,
                @error_severity,
                @error_state,
                @message,
                GETUTCDATE(),
                @pipeline_name,
                @pipeline_run_id,
                @pipeline_name,
                @pipeline_trigger_id,
                @pipeline_trigger_type,
                TRY_CAST(@pipeline_trigger_date_time_utc AS DATETIME2),
                1
            )
        END
        ELSE
            THROW
    END CATCH
END