from flask import render_template, request
from fmt import FMT

class ComparativaView:
    @staticmethod
    def comparativa():
        fmt = FMT()
        fmt.connect()

        documento = request.form.get('documento', '').strip()

        nuevo_data = fmt.get_nuevo_data(documento)
        viejo_data = fmt.get_viejo_data(documento)
        
        fmt.close()

        print(f"Nuevo data: {nuevo_data}")
        print(f"Viejo data: {viejo_data}")

        # Verificar que las claves existen en los datos
        required_keys = {'id', 'documento', 'objectid', 'ct', 'afectados'}
        if not nuevo_data or not required_keys.issubset(nuevo_data[0].keys()):
            return render_template('error.html', message="Los datos de 'nuevo' no tienen las claves requeridas.")
        if not viejo_data:
            return render_template('comparativa.html', nuevo_data=nuevo_data, viejo_only=[], summary={"nuevo_ct": len(nuevo_data), "nuevo_afectados": sum(int(row['afectados']) for row in nuevo_data), "viejo_ct": 0, "viejo_afectados": 0}, message="No hay datos en el conjunto 'viejo' para comparar.")
        if not required_keys.issubset(viejo_data[0].keys()):
            return render_template('error.html', message="Los datos de 'viejo' no tienen las claves requeridas.")

        # Convertir a diccionarios para acceso rápido
        def normalize(value):
            return str(value).strip().lower()

        # Convertir los datos a diccionarios para acceso rápido
        viejo_dict = {(normalize(row['id']), normalize(row['objectid']), normalize(row['documento'])): row for row in viejo_data}
        nuevo_dict = {(normalize(row['id']), normalize(row['objectid']), normalize(row['documento'])): row for row in nuevo_data}

        intersect_keys = set(nuevo_dict.keys()) & set(viejo_dict.keys())

        nuevo_data_with_status = []
        for key, row in nuevo_dict.items():
            if key in intersect_keys:
                viejo_row = viejo_dict[key]
                differences = {k: (row[k], viejo_row[k]) for k in required_keys if row[k] != viejo_row[k]}
                status = 'green' if not differences else 'yellow'
            else:
                status = 'red'
            row['status'] = status
            nuevo_data_with_status.append(row)

        viejo_only = [row for key, row in viejo_dict.items() if key not in intersect_keys]

        summary = {
            "nuevo_ct": len(nuevo_data),
            "nuevo_afectados": sum(int(normalize(row['afectados'])) for row in nuevo_data),
            "viejo_ct": len(viejo_data),
            "viejo_afectados": sum(int(normalize(row['afectados'])) for row in viejo_data),
        }
        
        return render_template('comparativa.html', nuevo_data=nuevo_data_with_status, viejo_only=viejo_only, summary=summary, message=None)
