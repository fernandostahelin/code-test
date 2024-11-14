CREATE OR REPLACE PROCEDURE process_data(
    IN p_fail_step INTEGER DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_execution_num INTEGER;
    v_start_time TIMESTAMP;
    v_step INTEGER;
    v_sleep_duration INTEGER;
    v_error_message TEXT;
BEGIN
    -- Get next execution number
    SELECT COALESCE(MAX(execution_num), 0) + 1 
    INTO v_execution_num 
    FROM long_process_data;

    -- Process steps 1-10
    FOR v_step IN 1..10 LOOP
        -- Log step start
        v_start_time := CURRENT_TIMESTAMP;
        INSERT INTO process_execution_log (execution_num, step_num, start_time, status)
        VALUES (v_execution_num, v_step, v_start_time, 'RUNNING');

        -- Simulate random processing time (1-5 minutes)
        v_sleep_duration := floor(random() * 5 + 1);
        PERFORM pg_sleep(v_sleep_duration * 60);  -- Convert to minutes

        -- Check if this step should fail
        IF v_step = p_fail_step THEN
            RAISE EXCEPTION 'Simulated failure at step %', v_step;
        END IF;

        -- Process dependencies
        IF v_step IN (3,4,5) THEN
            -- Steps 3,4,5 are dependent
            IF EXISTS (
                SELECT 1 
                FROM process_execution_log 
                WHERE execution_num = v_execution_num 
                AND step_num IN (3,4) 
                AND status = 'FAILED'
            ) THEN
                -- Rollback previous steps if necessary
                IF v_step = 4 THEN
                    DELETE FROM long_process_data 
                    WHERE execution_num = v_execution_num 
                    AND sub_process_desc = v_execution_num || '-3';
                END IF;
                CONTINUE;
            END IF;
        ELSIF v_step IN (7,8) THEN
            -- Steps 7,8 are dependent
            IF EXISTS (
                SELECT 1 
                FROM process_execution_log 
                WHERE execution_num = v_execution_num 
                AND step_num = 7 
                AND status = 'FAILED'
            ) THEN
                CONTINUE;
            END IF;
        ELSIF v_step = 10 THEN
            -- Step 10 depends on 3,4,5,7,8
            IF EXISTS (
                SELECT 1 
                FROM process_execution_log 
                WHERE execution_num = v_execution_num 
                AND step_num IN (3,4,5,7,8) 
                AND status = 'FAILED'
            ) THEN
                CONTINUE;
            END IF;
        END IF;

        -- Insert successful step data
        INSERT INTO long_process_data (
            execution_num,
            sub_process_desc,
            record_create_dtm,
            record_create_username
        )
        VALUES (
            v_execution_num,
            v_execution_num || '-' || v_step,
            CURRENT_TIMESTAMP,
            CURRENT_USER
        );

        -- Update log with success
        UPDATE process_execution_log 
        SET status = 'COMPLETED',
            end_time = CURRENT_TIMESTAMP
        WHERE execution_num = v_execution_num 
        AND step_num = v_step;

    END LOOP;

EXCEPTION WHEN OTHERS THEN
    -- Log the error
    GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
    
    UPDATE process_execution_log 
    SET status = 'FAILED',
        end_time = CURRENT_TIMESTAMP,
        error_message = v_error_message
    WHERE execution_num = v_execution_num 
    AND step_num = v_step;

    RAISE NOTICE 'Process failed at step % with error: %', v_step, v_error_message;
END;
$$;