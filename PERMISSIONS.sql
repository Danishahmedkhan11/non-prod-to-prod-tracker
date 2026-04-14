-- ====================================================================
-- PERMISSIONS FOR NON-PROD TO PROD TRACKER APP
-- ====================================================================
-- 1. Create/Deploy the App in the Databricks UI first.
-- 2. Go to the "Authorization" tab in your App settings to find 
--    the Service Principal ID (e.g., 'app-12345...').
-- 3. Run the following commands as an Account Admin or Catalog Admin.

-- Replace <service_principal_id> with the ID from Step 2.
-- Example: SET VAR app_sp = 'app-f7a8b...';

-- Grant usage on the system catalog
GRANT USE CATALOG ON CATALOG system TO `<service_principal_id>`;

-- Grant usage and select on required schemas
GRANT USE SCHEMA ON SCHEMA system.access TO `<service_principal_id>`;
GRANT SELECT ON SCHEMA system.access TO `<service_principal_id>`;

-- If you want to track query history as well:
GRANT USE SCHEMA ON SCHEMA system.query TO `<service_principal_id>`;
GRANT SELECT ON SCHEMA system.query TO `<service_principal_id>`;

-- Ensure the app has permissions to use the SQL Warehouse
-- This is usually done via the UI: Permissions -> Add -> Service Principal -> "Can Use"
