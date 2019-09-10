import xlrd, pymssql

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

def getdoc(ID):
    global cursor
    cursor.execute('SELECT Abstract FROM [NOSQL_db].[dbo].[LangFilterIEEE] WHERE ID = %d' % ID)
    for item in cursor:
        return str(item)
    cursor.execute('SELECT Abstract FROM [NOSQL_db].[dbo].[LangFilter] WHERE ID = %d' % ID)
    for item in cursor:
        return str(item)

# Give the location of the file 
loc = ("./t.xlsx") 
  
# To open Workbook 
wb = xlrd.open_workbook(loc) 
sheet = wb.sheet_by_index(0) 

for i in range(1, 81):
    # temp = ""
    for j in range(4, 9):
        cell = sheet.cell_value(i, j)
        if (str(cell) == ""):
            # temp = "None"
            break
        id = int(float(str(cell)))
        try:
            with open("excel_item_abstracts/%d.txt" % id, "x") as f:
                print("%s" % getdoc(id), file=f)
        except FileExistsError:
            pass
