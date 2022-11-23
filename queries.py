def query_create_table(table_name):
    return ('CREATE TABLE if not exists public."'+table_name+'" ' +
    '(' +
    ' "ID" SERIAL PRIMARY KEY, '+
    ' "Mod_Age" double precision,' +
    ' "Dormant_Age_Sort" integer,'+
    ' "Dormant_Age_Group" character varying(1000),'+
    ' "Dormant_Age" double precision,'+
    ' "File_Age_Sort" integer,'+
    ' "File_Age_Group" character varying(1000),'+
    ' "File_Age" double precision,'+
    ' "File_Size_Group" character varying(1000),'+
    ' "File_Size" double precision,'+
    ' "Last_Write_Time" integer,'+
    ' "Last_Access_Time" integer,'+
    ' "Creation_Time" integer,'+
	' "Drive_Label" character varying(1000),'+
    ' "Username" character varying(1000),'+
    ' "Drive_Serial_Number" character varying(1000),'+
    ' "File_Size_Sort" integer,'+
    ' "System_File" BOOLEAN,'+
    ' "Extension" character varying(1000),'+
    ' "Server_Name" character varying(1000),'+
    ' "User_Group" character varying(1000),'+
    ' "File_Depth" integer,'+
    ' "Directory" character varying(1000),'+
    ' "File_Name" character varying(1000),'+
    ' "File_Category" character varying(1000),'+
    ' "Compressed" character varying(1000)'+
    ');')




    