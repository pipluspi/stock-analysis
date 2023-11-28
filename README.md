# stock-analysis
Scrape stock, eft, fed_funds, ntfs from different website like barchart, investing, trandingeconomics etc.

# How to setup on local
**Step 1 :** Clone or Download zip of repo and Unzip the file that you have downloaded

**Step 2 :** Make **'config.ini'** in same directory 
    Add :
        [<name you like>]
        mysql_connection_string = your sql alchemy string of your mysql database

**Step 3 :** Then add this line to all python file (Replace mysql_connection_string with below):
    url = config.get('name given in above step','mysql_connection_string')