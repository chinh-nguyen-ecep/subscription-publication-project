* * * * * psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_export_data_file()" >/dev/null 2>&1
* * * * * sleep 10;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_export_data_file()" >/dev/null 2>&1
* * * * * sleep 30;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_export_data_file()" >/dev/null 2>&1
* * * * * sleep 40;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_export_data_file()" >/dev/null 2>&1
* * * * * sleep 50;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_export_data_file()" >/dev/null 2>&1

* * * * * psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_public_export_file()" >/dev/null 2>&1
* * * * * slepp 30;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_public_export_file()" >/dev/null 2>&1

* * * * * psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_update_subscription_process_status()"  >/dev/null 2>&1
* * * * * sleep 20;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_update_subscription_process_status()"  >/dev/null 2>&1
* * * * * sleep 20;psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_update_subscription_process_status()"  >/dev/null 2>&1


* * * * * psql -d analyticsdb -c "SELECT * FROM control.fn_spctl_refresh_update_data_up_to_date()"  >/dev/null 2>&1