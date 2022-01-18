import copy
import operator
from numbers import Number
import datetime

def dict_agregar_fila(diccionario_al_que_agrego,fila_a_appendear):
    for keys in fila_a_appendear.keys():
        diccionario_al_que_agrego[keys].append(fila_a_appendear[keys])
    return diccionario_al_que_agrego


def str_to_datetime(fecha,formato_entrada):
    fecha_datetime = datetime.datetime.strptime(fecha, formato_entrada)
    return fecha_datetime

def datetime_to_str(fecha_datetime,formato_salida):
    fecha_str = datetime.datetime.strftime(fecha_datetime, formato_salida)
    return fecha_str


def crearDataFrame(guia = None, valores = None):
    filas = 0
    tabla = {}
    columnas = {}

    tabla["Indice"] = []

    if guia:
        if valores:
            tabla["Indice"] = guia["Indice"]
            for columna in guia.keys():

                if columna != "Indice":
                    columnas[columna] = guia[columna]
                    tabla[columna] = guia[columna]

            tabla.update(columnas)
            df = Dataframe(tabla, len(tabla["Indice"]), list(columnas.keys()))

        else:

            for columna in guia.keys():    

                if columna != "Indice":
                    columnas[columna] = []

            tabla.update(columnas)
            df = Dataframe(tabla, filas, columnas)

    else:
        df = Dataframe(tabla, filas, columnas)        
    
    return df



def definirColumnas(archivo, separador):
    columnas = {}
    linea = archivo.readline()
    archivo.seek(0)
    cantidadColumnas = linea.count(separador) + 1

    for indice in range(cantidadColumnas):
        columnas["col" + str(indice)] = []
    
    return columnas 



def ejecutarOperacion(valor, operacion):
    operador = operacion[0]
    valor2 = operacion[1]
    
    return dictOperador[operador](valor, valor2)



def leer_csv(archivo, separador):
    with open (archivo, "r") as f:
        tabla = {}
        filas = 0
        tabla["Indice"] = []
        columnas = definirColumnas(f, separador)
        tabla.update(columnas)

        for line in f:
            tabla["Indice"].append(filas)

            for index, parametro in enumerate(line.split(separador)):                
                if "\n" in str(parametro):
                    parametro = parametro.replace("\n", "")
                tabla["col" + str(index)].append(parametro)   
            filas += 1
        df = Dataframe(tabla, filas, columnas)
        
        return df 



class Operadores:
    igualdad = "=="
    desigualdad = "!="
    menor = "<"
    menorIgual = "<="
    mayor = ">"
    mayorIgual = ">="
    suma = "+"
    resta = "-"
    multiplicacion = "*"
    division = "/"



    @classmethod
    def funcIgual(cls, *args):
        return args[0] == args[1]



    @classmethod
    def funcDistinto(cls, *args):
        return args[0] != args[1]



    @classmethod
    def funcMenor(cls, *args):
        return args[0] < args[1]



    @classmethod
    def funcMenorIgual(cls, *args):
        return args[0] <= args[1]



    @classmethod
    def funcMayor(cls, *args):
        return args[0] > args[1]



    @classmethod
    def funcMayorIgual(cls, *args):
        return args[0] >= args[1]



    @classmethod
    def funcSuma(cls, *args):
        return args[0] + args[1]



    @classmethod
    def funcResta(cls, *args):
        return args[0] - args[1]



    @classmethod
    def funcMultiplicacion(cls, *args):
        return args[0] * args[1]



    @classmethod
    def funcDivision(cls, *args):
        return args[0] / args[1]



dictOperador = {
    Operadores.igualdad: Operadores.funcIgual,
    Operadores.desigualdad: Operadores.funcDistinto,
    Operadores.menor: Operadores.funcMenor,
    Operadores.menorIgual: Operadores.funcMenorIgual,
    Operadores.mayor: Operadores.funcMayor,
    Operadores.mayorIgual: Operadores.funcMayorIgual,
    Operadores.suma: Operadores.funcSuma,
    Operadores.resta: Operadores.funcResta,
    Operadores.multiplicacion: Operadores.funcMultiplicacion,
    Operadores.division: Operadores.funcDivision
}



class Dataframe:
    def __init__(self, df, filas, columnas):
        self.df = df
        self.filas = filas
        self.columnas = list(columnas.keys()) if isinstance(columnas, dict) else columnas



    def __repr__(self):
        print(self.df)   

        return ""



    def acumular(self, columna, nuevaCol):
        acumulador = 0
        self.agregarCol(nuevaCol, False)

        for valor in self.df[columna]:
            acumulador += valor
            self.df[nuevaCol].append(acumulador)

        

    def agregarCol(self, nuevaCol, rellenar = True): #observar que tome listas ademรกs de valores individuales
        if isinstance(nuevaCol,list):
            for cadaCol in nuevaCol:
                self.df[cadaCol] = []
                if cadaCol not in self.columnas:
                    self.columnas.append(cadaCol)
                
                if rellenar:
                    for i in range(self.filas):
                        self.df[cadaCol].append("")

        else:
            self.df[nuevaCol] = []
            if nuevaCol not in self.columnas:
                self.columnas.append(nuevaCol)

            if rellenar:

                for i in range(self.filas):
                    self.df[nuevaCol].append("")



    def agregarFila(self, otro, indices = False):
        
        
        if isinstance(otro, dict):
            for columna in otro.keys():
                if not self.vacio():
                    #if type(self.df[columna][0]) == type(otro[columna]) or isinstance(self.df[columna][0], Number) == isinstance(otro[columna], Number):
                    self.df[columna].append(otro[columna])
                    #else:
                    #    raise TypeError("El tipo de dato ingresado no coincide con el de la columna: ", columna, " - ", type(otro[columna]), ":", type(self.df[columna][0]))            
                else:
                    self.df[columna].append(otro[columna])


            if "Indice" not in otro.keys():
                if self.vacio():
                    self.df["Indice"].append(0)
                else:
                    self.df["Indice"].append(self.df["Indice"][-1]+1)     

            for columna in self.columnas:
                if columna not in otro.keys():              
                    if self.df[columna]:
                        if type(self.df[columna][0]) == str:
                            self.df[columna].append('')
                        else:
                            self.df[columna].append(0)
                    else:
                        self.df[columna].append('')
            
            self.filas = self.filas + 1

            if indices:
                self.reiniciarIndices(inplace = True)
            
        elif isinstance(otro, list):
            if not self.vacio():
                self.df["Indice"].append(self.df["Indice"][-1]+1)
            else:
                self.df["Indice"].append(0)
                
            if len(otro) <= len(self.columnas):
                for index, columna in enumerate(self.columnas):
                    if index < len(otro):
                        if not self.vacio():
                            if type(self.df[columna][0]) == type(otro[index]) or isinstance(self.df[columna][0], Number) == isinstance(otro[index], Number):
                                self.df[columna].append(otro[index])
                            else:
                                raise TypeError("El tipo de dato ingresado no coincide con el de la columna: ", columna, " - ", type(otro[index]), ":", type(self.df[columna][0]))
                        else:
                            self.df[columna].append(otro[index])
                    else:
                        if self.df[columna]:
                            if type(self.df[columna][0]) == str:
                                self.df[columna].append('')
                            else:
                                self.df[columna].append(0)                                   
                        else:
                            self.df[columna].append('')
                
                self.filas = self.filas + 1
            else:
                raise RuntimeError("Se esta intentando ingresar mas datos que columnas: ", len(otro), ":", len(self.columnas))
            
        

        


   
    def agrupar(self, columnas):
        dfActual = copy.deepcopy(self)


        while not dfActual.vacio():
            pivot = next(dfActual.iterar())
            listaAgrupacion = columnas.copy()
            columna = listaAgrupacion.pop(0)
            claveActual = pivot[columna]

            dfEquivalentes = dfActual.buscar({columna: ["==", claveActual]})
            dfDiferentes = dfActual.buscar({columna: ["!=", claveActual]})

            if listaAgrupacion:
                for clave, df in dfEquivalentes.agrupar(listaAgrupacion):
                    listaClaves = []
                    listaClaves.append(claveActual)
                    listaClaves = listaClaves + clave
                    yield listaClaves, df
            else:
                listaClaves = [claveActual]
                yield listaClaves, dfEquivalentes

            dfActual = dfDiferentes



    def anexar(self, otro, indices = False):
        df = copy.deepcopy(self)

        if isinstance(otro, Dataframe):
            for fila in otro.iterar():
                df.agregarFila(fila, indices)
        elif isinstance(otro, dict) or isinstance(otro, list):
            df.agregarFila(otro)

        return df



    def buscar(self, condiciones = None):
        filasAdmitidas = []
        filas = copy.deepcopy(self.filas)
        tabla = copy.deepcopy(self.df)

        if condiciones:
            filas = 0

            for fila in self.iterar():
                condicion = True

                for columna in condiciones.keys():
                    if not ejecutarOperacion(fila[columna], condiciones[columna]):
                        condicion = False
                        break

                if condicion:
                    filasAdmitidas.append(copy.deepcopy(fila))               
                    filas += 1

            for columna in tabla.keys():
                tabla[columna].clear()            

            for fila in filasAdmitidas:
                tabla["Indice"].append(fila["Indice"])

                for columna in self.columnas:
                    tabla[columna].append(fila[columna])

        copyDf = Dataframe(tabla, filas, copy.deepcopy(self.columnas))

        return copyDf



    def cambiarColumnas(self, lista):
        for indice in range(len(lista)):
            self.df[lista[indice]] = self.df.pop(self.columnas[indice])
            self.columnas[indice] = lista[indice]

 

    def cambiarValor(self, columna, valor, inplace = False, condiciones = None):
        #valor = [["+", 15], ["-", 4], ["/", 2]]
        if condiciones:            
            df = self.buscar(condiciones)
        else:            
            if inplace:
                df = self
            else:
                df = copy.deepcopy(self)

        if isinstance(valor, list):
            if isinstance(valor[0], list):
                for i in range(df.filas):
                    for numOperacion in range(len(valor)): 
                        df.df[columna][i] = ejecutarOperacion(df.df[columna][i], valor[numOperacion])
            else:
                for i in range(df.filas):
                    df.df[columna][i] = valor[i] if i < len(valor) else df.df[columna][i]
        else:
            for i in range(df.filas):
                df.df[columna][i] = valor

    
        if condiciones:
            filasCambiar = copy.deepcopy(df)

            if inplace:                
                df = self
            else:
                df = copy.deepcopy(self)           

            for fila in filasCambiar.iterar():
                for i in range(df.filas):
                    if df.df["Indice"][i] == fila["Indice"]:
                        df.df[columna][i] = fila[columna]
            if not inplace:
                return df
        elif not inplace:
            return df



    def cambiarTipo(self, diccionarioCambio): 
        for columna in diccionarioCambio.keys():
            for indice, item in enumerate(self.df[columna]):
                if isinstance(item, str) and item == "" and (diccionarioCambio[columna] == int or diccionarioCambio[columna] == float):
                    self.df[columna][indice] = "0"
            nuevaLista = list(map(diccionarioCambio[columna], self.df[columna]))
            self.df[columna] = nuevaLista



    def contiene(self, columna, valor):
        listaAceptados = [valor in item for item in self.df[columna]]

        return listaAceptados



    def copiar(self, deep = True):
        if deep:
            df = copy.deepcopy(self)
        else:
            df = copy.copy(self)
        return df



    def eliminar(self, indices = None, columnas = None, inplace = False):
        if inplace:
            df = self
        else:
            df = copy.deepcopy(self)

        if indices:
            for fila in reversed(range(df.filas)):
                if df.df["Indice"][fila] in indices:
                    df.df["Indice"].pop(fila)

                    for columna in df.columnas:
                        df.df[columna].pop(fila)
                    
                    df.filas = df.filas - 1

        if columnas:
            for columna in reversed(df.columnas):
                if columna in columnas:
                    del df.df[columna]
                    df.columnas.remove(columna)

        if not inplace:
            return df
        


    def exportarCSV(self, ruta, separador = ",", columnas = False, indice = False, how = None, filling = None):
        #se elige la forma de abrir el archivo, "w" de escritura, "a" para appendear, etc...
        if filling:
            way = filling

        else:
            way = "w"

        with open(ruta, way) as file:
            if how == None:
                if columnas:
                    if indice:
                        file.write("Indice" + separador)
                    file.write(separador.join(self.columnas) + "\n")

                for fila in self.iterar():
                    texto = []
                    if indice:
                        texto.append(str(fila["Indice"]))
                
                        for columna in self.columnas:
                            texto.append(str(fila[columna]))

                    else:
                        for columna in self.columnas:
                            texto.append(str(fila[columna]))

                    file.write(separador.join(texto) + "\n")

            elif how == "ne":
            #ne por "no estructurado"
                for fila in self.iterar():
                    for columna in self.columnas: 
                        file.write(str(fila[columna]) + "\n")

            file.close()


    def final(self, cantidad = 5, columnasSelect = None):
        copyDf = copy.deepcopy(self)
        columnasEliminar = []
        i = 0

        if columnasSelect:
            for columna in copyDf.columnas:
                if columna not in columnasSelect:
                    columnasEliminar.append(columna)
            copyDf = copyDf.eliminar(columnas = columnasEliminar)
        else:
            columnasSelect = copyDf.columnas

        tabla = copyDf.df

        for columna in tabla.keys():
            tabla[columna].clear()

        for fila in self.iterar():
            if i < self.filas - cantidad:
                pass
            else:   
                tabla["Indice"].append(fila["Indice"])         

                for columna in copyDf.columnas:
                    tabla[columna].append(fila[columna])
            i += 1


        copyDf = Dataframe(tabla, cantidad, columnasSelect)
        
        return copyDf



    def indices(self):       
        indices = []

        for i in range(self.filas):
            indices.append(self.df["Indice"][i])
        
        return indices



    def iterar(self, *args):
        cont = 0
        fila = {}
        columnasSelect = []

        if args:
            for columna in args:
                if columna not in self.columnas:
                    pass
                else:
                    columnasSelect.append(columna)  
        else:
            columnasSelect = self.columnas

        while cont < self.filas:
            fila["Indice"] = self.df["Indice"][cont]

            for columna in columnasSelect:
                fila[columna] = self.df[columna][cont]

            yield fila

            cont += 1



    def ordenar(self, columnas, ascendente = True, indices = False):
        if self.filas > 1 and columnas:
            pivot = next(self.iterar())
            listaOrdenamiento = columnas.copy()
            columna = listaOrdenamiento.pop(0)

            dfMenores = self.buscar({columna : ["<", pivot[columna]]})
            dfPivot = self.buscar({columna : ["==", pivot[columna]]})
            dfMayores = self.buscar({columna : [">", pivot[columna]]})

            dfMenores = dfMenores.ordenar(columnas, ascendente, indices)
            dfMayores = dfMayores.ordenar(columnas, ascendente, indices)
            dfPivot = dfPivot.ordenar(listaOrdenamiento, ascendente, indices)

            if ascendente:
                if dfMenores.vacio():
                    dfMenores = dfPivot
                else:    
                    dfMenores = dfMenores.anexar(dfPivot)
                if not dfMayores.vacio(): 
                    dfMenores = dfMenores.anexar(dfMayores)
                df = dfMenores
            else:
                if dfMayores.vacio():
                    dfMayores = dfPivot
                else:
                    dfMayores = dfMayores.anexar(dfPivot)

                if not dfMenores.vacio():
                    dfMayores = dfMayores.anexar(dfMenores)

                df = dfMayores
            
            if indices:
                df.reiniciarIndices(True)

            return df
        else:
            return self
        


    def principio(self, cantidad = 5, columnasSelect = None):
        copyDf = copy.deepcopy(self)
        columnasEliminar = []
        i = 0

        if columnasSelect:
            for columna in copyDf.columnas:
                if columna not in columnasSelect:
                    columnasEliminar.append(columna)
            copyDf = copyDf.eliminar(columnas = columnasEliminar)
        else:
            columnasSelect = copyDf.columnas

        tabla = copyDf.df

        for columna in tabla.keys():
            tabla[columna].clear()

        for fila in self.iterar():
            if i >= cantidad:
                break
            else:
                tabla["Indice"].append(fila["Indice"])
                for columna in copyDf.columnas:
                    tabla[columna].append(fila[columna])
            i += 1

        copyDf = Dataframe(tabla, cantidad, columnasSelect)
        
        return copyDf



    def redondear(self, kwargs):
        for columna in kwargs.keys():
            if isinstance(self.df[columna][0], Number):
                for i in self.indices():
                    self.df[columna][i] = round(self.df[columna][i], kwargs[columna])
            else:
                raise TypeError("No se puede redondear un dato de tipo ", type(self.df[columna][0]))



    def reiniciarIndices(self, inplace = False):
        if inplace:
            df = self
        else:
            df = copy.deepcopy(self)

        df.df["Indice"].clear()

        for iNum in range(self.filas):
            df.df["Indice"].append(iNum)

        if not inplace:
            return df



    def remplazar(self, columna, cadena, remplazo):
        nuevaLista = [item.replace(cadena, remplazo) for item in self.df[columna]]
        self.df[columna] = nuevaLista



    def separar(self, columna, valor, posicion = None):
        listaSplits = [(item.split(valor)[posicion] if posicion != None else item.split(valor)) for item in self.df[columna]]
        
        return listaSplits

 

    def vacio(self):
        if self.filas:
            return False
        else:
            return True



    def valores(self, *args):
        listaValores = []
        cont = 0
        columnasSelect = []

        if args:
            for columna in args:
                if columna not in self.columnas:
                    pass
                else:
                    columnasSelect.append(columna)
                    listaValores.append([])  
        else:
            columnasSelect = copy.deepcopy(self.columnas)

            for i in range(len(self.columnas)):
                listaValores.append([])


        while cont < self.filas:
            for posc, columna in enumerate(columnasSelect):
                listaValores[posc].append(copy.deepcopy(self.df[columna][cont]))
                self
            cont += 1

        return listaValores


    #------------------------------------

    def colMax(self, colummax):
        pivot = next(self.iterar())
        if isinstance(pivot[colummax],Number):
            maximo = pivot[colummax]
            for dicc in self.iterar(): 
                maximo = maximo if maximo > dicc[colummax] else dicc[colummax]
            return maximo
        else:
            raise TypeError("No se puede comparar el tipo de dato {} ".format(type(pivot[colummax])))

    def colMin(self, colummin): 
        pivot = next(self.iterar())
        if isinstance(pivot[colummin],Number):
            minimo = pivot[colummin]
            for dicc in self.iterar(): 
                minimo = minimo if minimo < dicc[colummin] else dicc[colummin]
            return minimo
        else:
            raise TypeError("No se puede comparar el tipo de dato {} ".format(type(pivot[colummin])))


    def Acum(self, columacum):
        acum = 0
        for dicc in self.iterar():
            acum += dicc[columacum]
        return acum


    def unique(self, columunique):
        unique = []
        for i in range(self.filas):
            if self.df[columunique][i] in unique:
                pass
            else:
                unique.append(self.df[columunique][i])
        return unique


    def nunique(self, columunique):
        nunique = self.unique(columunique)
        return len(nunique)


    def mean(self, colummean): 
        if self.filas != 0 :
            return self.Acum(colummean)/self.filas
        else:
            raise TypeError("No se puede dividir por 0 ", self.filas)


    def innerJoin(self, dfderecha , colummatch, columnas_izq = False, columnas_der = False, how = None):
        dataframe_nuevo = crearDataFrame()

        if columnas_izq:
            for columna in columnas_izq:
                if columna not in self.columnas:
                    raise TypeError("La columna {} no se encuentra en las columnas del DataFrame Izquierdo".format(columna))

            lista_columnas_izquierdo = columnas_izq

        else:
            lista_columnas_izquierdo = self.columnas



        if columnas_der:
            for columna in columnas_der:
                if columna not in dfderecha.columnas:
                    raise TypeError("La columna {} no se encuentra en las columnas del DataFrame Derecho".format(columna))

            lista_columnas_derecho = columnas_der

        else:
            lista_columnas_derecho = dfderecha.columnas



        for columna in lista_columnas_izquierdo: 
            dataframe_nuevo.agregarCol(columna, rellenar = False) 
        for columna in lista_columnas_derecho:
            dataframe_nuevo.agregarCol(columna, rellenar = False)
       
        
        if how == "left":
            for fila in self.iterar(): 
                dfderecha_copia = dfderecha.copiar()
                lista_keys = list(colummatch.keys())

                while lista_keys:
                    key = lista_keys.pop(0)
                    columnaderecha = colummatch[key]
        #------------------------------------# INICIO LEFT OUTTER

                    if fila[key] not in dfderecha.df[key]:
                        lista_valores = []

                        for columna in lista_columnas_izquierdo:
                            lista_valores.append(fila[columna])

                        dataframe_nuevo = dataframe_nuevo.anexar(lista_valores, indices= True)
        #------------------------------------# FIN LEFT OUTTER

        #------------------------------------# INICIO INNER
                    if fila[key] in dfderecha.df[key]:
                        dfderecha_copia = dfderecha_copia.buscar({columnaderecha :["==", fila[key]]}) #busco donde se cumple la condiciรณn sobre la columna solicitada

                        if not dfderecha_copia.vacio(): #sino estรก vacรญo
                            
                            for recorriendo_dfderecha in dfderecha_copia.iterar(): #recorro cada fila en caso que el df traiga mรกs

                                lista_valores = []

                                for columna in lista_columnas_izquierdo:
                                    lista_valores.append(fila[columna])

                                for columna in lista_columnas_derecho: #Usar Lower o Upper para verificar que no estรฉ en minรบsculas o mayรบsculas
                                    if columna not in lista_columnas_izquierdo:
                                        lista_valores.append(recorriendo_dfderecha[columna])
                    
                                dataframe_nuevo = dataframe_nuevo.anexar(lista_valores, indices= True)
        #------------------------------------# FINAL INNER
        elif how == "inner" or how == None:

            for fila in self.iterar(): 
                dfderecha_copia = dfderecha.copiar()
                lista_keys = list(colummatch.keys())

                while lista_keys:
                    key = lista_keys.pop(0)
                    columnaderecha = colummatch[key]
        #------------------------------------# INICIO INNER
                    if fila[key] in dfderecha.df[key]:
                        dfderecha_copia = dfderecha_copia.buscar({columnaderecha :["==", fila[key]]}) #busco donde se cumple la condiciรณn sobre la columna solicitada

                        if not dfderecha_copia.vacio(): #sino estรก vacรญo
                            
                            for recorriendo_dfderecha in dfderecha_copia.iterar(): #ro cada fila en caso que el df traiga mรกs

                                lista_valores = []

                                for columna in lista_columnas_izquierdo:
                                    lista_valores.append(fila[columna])

                                for columna in lista_columnas_derecho: #Usar Lower o Upper para verificar que no estรฉ en minรบsculas o mayรบsculas
                                    if columna not in lista_columnas_izquierdo:
                                        lista_valores.append(recorriendo_dfderecha[columna])
                                dataframe_nuevo = dataframe_nuevo.anexar(lista_valores, indices= True)
        #------------------------------------# FINAL INNER

        elif how == "outer":

            for fila in self.iterar(): 
                dfderecha_copia = dfderecha.copiar()
                lista_keys = list(colummatch.keys())

                while lista_keys:
                    key = lista_keys.pop(0)
                    columnaderecha = colummatch[key]
        #------------------------------------# INICIO LEFT OUTTER

                    if fila[key] not in dfderecha.df[key]:
                        lista_valores = []

                        for columna in lista_columnas_izquierdo:
                            lista_valores.append(fila[columna])

                        dataframe_nuevo = dataframe_nuevo.anexar(lista_valores, indices= True)
        #------------------------------------# FIN LEFT OUTTER

        #------------------------------------# INICIO INNER
                    if fila[key] in dfderecha.df[key]:
                        dfderecha_copia = dfderecha_copia.buscar({columnaderecha :["==", fila[key]]}) 

                        if not dfderecha_copia.vacio():
                            
                            for recorriendo_dfderecha in dfderecha_copia.iterar(): 

                                lista_valores = []

                                for columna in lista_columnas_izquierdo:
                                    lista_valores.append(fila[columna])

                                for columna in lista_columnas_derecho: 
                                    if columna not in lista_columnas_izquierdo:
                                        lista_valores.append(recorriendo_dfderecha[columna])
                    
                                dataframe_nuevo = dataframe_nuevo.anexar(lista_valores, indices= True)
        #------------------------------------# FINAL INNER

            dfderecha_copia2 = dfderecha.copiar()
            #------------------------------------# INICIO OUTER RIGHT
            for fila in self.iterar(): 
                lista_keys = list(colummatch.keys())

                while lista_keys:
                    key = lista_keys.pop(0)
                    columnaderecha = colummatch[key]
                    dfderecha_copia2 = dfderecha_copia2.buscar({columnaderecha :["!=", fila[key]]})
                    
            dataframe_nuevo = dataframe_nuevo.anexar(dfderecha_copia2, indices= True)
                #------------------------------------# FIN OUTER RIGHT

        return dataframe_nuevo
