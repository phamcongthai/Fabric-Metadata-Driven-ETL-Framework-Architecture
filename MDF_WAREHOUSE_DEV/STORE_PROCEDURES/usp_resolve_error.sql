CREATE OR ALTER   PROCEDURE [mdf_platform_orchestration].[usp_resolve_error]
    @table_name     VARCHAR(500),
    @pipeline_name  VARCHAR(500),
    @layer          VARCHAR(200)
AS
BEGIN
    DECLARE @next_id BIGINT

    BEGIN TRY
        SELECT @next_id = ISNULL(MAX(log_id), 0) + 1
        FROM mdf_platform_orchestration.error_log_history

        INSERT INTO mdf_platform_orchestration.error_log_history
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
        SELECT
            @next_id,
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
            0
        FROM mdf_platform_orchestration.error_log
        WHERE object_name LIKE '%' + @table_name + '%'
        AND active_flag = 1

        UPDATE mdf_platform_orchestration.error_log
        SET active_flag = 0
        WHERE object_name LIKE '%' + @table_name + '%'
        AND active_flag = 1
    END TRY

    BEGIN CATCH
        IF ERROR_NUMBER() = 24556
        BEGIN
            SELECT @next_id = ISNULL(MAX(log_id), 0) + 1
            FROM mdf_platform_orchestration.error_log_history

            INSERT INTO mdf_platform_orchestration.error_log_history
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
            SELECT
                @next_id,
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
                0
            FROM mdf_platform_orchestration.error_log
            WHERE object_name LIKE '%' + @table_name + '%'
            AND active_flag = 1

            UPDATE mdf_platform_orchestration.error_log
            SET active_flag = 0
            WHERE object_name LIKE '%' + @table_name + '%'
            AND active_flag = 1
        END
        ELSE
            THROW
    END CATCH
END