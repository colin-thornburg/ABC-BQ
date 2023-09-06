DECLARE 
    -- Declare variables to hold information from the tables
    v_patient_id patients.Patient_ID%TYPE;
    v_patient_name patients.Patient_Name%TYPE;
    v_provider_name providers.Provider_Name%TYPE;
    v_specialty providers.Specialty%TYPE;
    v_claim_amount claims.Claim_Amount%TYPE;
    v_total_claim_amount NUMBER(10,2);
    v_avg_claim_amount NUMBER(10,2);
    v_claim_count NUMBER;
    
BEGIN
    -- Cursor to join all the tables and get necessary details
    FOR rec IN (SELECT 
                    p.Patient_ID, 
                    p.Patient_Name, 
                    pr.Provider_Name, 
                    pr.Specialty, 
                    c.Claim_Amount,
                    c.loaded_at
                FROM 
                    patients p
                JOIN 
                    claims c ON p.Patient_ID = c.Patient_ID
                JOIN 
                    providers pr ON pr.Provider_ID = c.Provider_ID)
    LOOP
        -- Assigning the values from the cursor to variables
        v_patient_id := rec.Patient_ID;
        v_patient_name := rec.Patient_Name;
        v_provider_name := rec.Provider_Name;
        v_specialty := rec.Specialty;
        v_claim_amount := rec.Claim_Amount;
        
        -- Nested Block to calculate total claim amount, average claim amount and claim count per provider
        DECLARE 
            v_inner_provider_name providers.Provider_Name%TYPE;
        BEGIN
            v_inner_provider_name := v_provider_name;
            
            -- Calculate the total claim amount per provider
            SELECT 
                SUM(Claim_Amount), 
                AVG(Claim_Amount), 
                COUNT(Claim_ID) 
            INTO 
                v_total_claim_amount, 
                v_avg_claim_amount, 
                v_claim_count
            FROM 
                claims c
            JOIN 
                providers pr ON pr.Provider_ID = c.Provider_ID
            WHERE 
                pr.Provider_Name = v_inner_provider_name;
            
            -- Output the results
            DBMS_OUTPUT.PUT_LINE('Patient ID: ' || v_patient_id);
            DBMS_OUTPUT.PUT_LINE('Patient Name: ' || v_patient_name);
            DBMS_OUTPUT.PUT_LINE('Provider Name: ' || v_provider_name);
            DBMS_OUTPUT.PUT_LINE('Specialty: ' || v_specialty);
            DBMS_OUTPUT.PUT_LINE('Claim Amount: ' || v_claim_amount);
            DBMS_OUTPUT.PUT_LINE('Total Claim Amount for Provider: ' || v_total_claim_amount);
            DBMS_OUTPUT.PUT_LINE('Average Claim Amount for Provider: ' || v_avg_claim_amount);
            DBMS_OUTPUT.PUT_LINE('Claim Count for Provider: ' || v_claim_count);
            DBMS_OUTPUT.PUT_LINE('-----------------------------');
        END;
    END LOOP;
END;
