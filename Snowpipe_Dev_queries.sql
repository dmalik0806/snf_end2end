-- Creating stages, storage integration, notification integration and pipes for the landing zone
-- when using Azure to store and fetch the files from, need to create storage integration and notification integration 
-- azure_tenant_id is microsoft Intra ID which we can see in our Azure subscription, it will be same for other storage integrations as well
create or replace storage integration order_az_integration
type = external_stage
storage_provider = azure
enabled = true
azure_tenant_id = 'c2fc7675-fe7a-421d-8a89-84d3941380cd'
storage_allowed_locations = ('azure://snowflakedemo1.blob.core.windows.net/ordersdata');

desc storage integration order_az_integration; 
-- click on the azure_consent_url and give permission to azure
--copy only the first part of the azure_multi_tenant_app_name from here -- pjwbdksnowflakepacint(till here only)_1744471010085
--azure_multi_tenant_app_name will be added at the Azure Storage account level
-- in the azure storage account, go to access control and add a role assignment.
-- In the Role tab search for 'Storage Blob' choose the appropriate role, in the next section click on add member and paste the app_name there.

create or replace storage integration item_az_integration
type = external_stage
storage_provider = azure
enabled = true
azure_tenant_id = 'c2fc7675-fe7a-421d-8a89-84d3941380cd'
storage_allowed_locations = ('azure://snowflakedemo1.blob.core.windows.net/itemdata');

create or replace storage integration customer_az_integration
type = external_stage
storage_provider = azure
enabled = true
azure_tenant_id = 'c2fc7675-fe7a-421d-8a89-84d3941380cd'
storage_allowed_locations = ('azure://snowflakedemo1.blob.core.windows.net/customerdata');

use schema landing_zone;
-- Creating stages in the landing_zone schema for customer, item and orders table
create stage delta_orders_azure
url = 'azure://snowflakedemo1.blob.core.windows.net/ordersdata'
storage_integration = order_az_integration      -- incase of using azure blob storage as a source need to add the storage integration
comment = 'feed delta order files';

create stage delta_items_azure
url = 'azure://snowflakedemo1.blob.core.windows.net/itemdata'
storage_integration = item_az_integration
comment = 'feed delta item files';

create stage delta_customers_azure
url = 'azure://snowflakedemo1.blob.core.windows.net/customerdata'
storage_integration = customer_az_integration
comment = 'feed delta customers files';

-- now notification integration will be created : https://snowflakedemo1.queue.core.windows.net/snowflakequeue
create or replace notification integration snowpipe_event
enabled = true
type = queue
notification_provider = azure_storage_queue
azure_storage_queue_primary_uri = 'https://snowflakedemo1.queue.core.windows.net/snowflakequeue'
azure_tenant_id = 'c2fc7675-fe7a-421d-8a89-84d3941380cd';

desc notification integration snowpipe_event;  -- give consent by clicking on link, copy the app name 'lczjwmsnowflakepacint_1744474266825'

-- after setting up storage integration, stages and notification_notification, will creating snow pipes
show stages;
create or replace pipe item_pipe
auto_ingest = true
integration = 'SNOWPIPE_EVENT'
as
copy into landing_item from @delta_items_azure
file_format = (type=csv compression=none)
pattern='.*item.*[.]csv'
on_error = 'CONTINUE';

create or replace pipe order_pipe
auto_ingest = true
integration = 'SNOWPIPE_EVENT'
as
copy into landing_order from @delta_orders_azure
file_format = (type=csv compression=none)
pattern='.*order.*[.]csv'
on_error = 'CONTINUE';

create or replace pipe customer_pipe
auto_ingest = true
integration = 'SNOWPIPE_EVENT'
as
copy into landing_customer from @delta_customers_azure
file_format = landing_zone_csv_format
pattern='.*customer.*[.]csv'
on_error = 'CONTINUE';

show pipes;

-- checking the status of snowpipes if they are active or not
select system$pipe_status('customer_pipe');
