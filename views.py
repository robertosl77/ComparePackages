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
        if not viejo_data or not required_keys.issubset(viejo_data[0].keys()):
            return render_template('error.html', message="Los datos de 'viejo' no tienen las claves requeridas.")

        # Convertir a diccionarios para acceso rápido
        def normalize(value):
            return str(value).strip().lower()

        viejo_dict = {normalize(row['id']): row for row in viejo_data}
        nuevo_dict = {normalize(row['id']): row for row in nuevo_data}

        intersect_ids = set(nuevo_dict.keys()) & set(viejo_dict.keys())

        nuevo_data_with_status = []
        for row in nuevo_data:
            normalized_row = {key: normalize(value) for key, value in row.items()}
            if normalized_row['id'] in intersect_ids:
                viejo_row = {key: normalize(value) for key, value in viejo_dict[normalized_row['id']].items()}
                differences = {key: (normalized_row[key], viejo_row[key]) for key in required_keys if normalized_row[key] != viejo_row[key]}
                if not differences:
                    status = 'green'  # Coincidente
                else:
                    # status = 'yellow'  # Mismo ID pero diferente en algún campo
                    # row['differences'] = differences
                    status = 'red'  # No está en los datos viejos
            else:
                status = 'red'  # No está en los datos viejos
            row['status'] = status
            nuevo_data_with_status.append(row)

        viejo_only = [row for row_id, row in viejo_dict.items() if row_id not in intersect_ids]
        
        summary = {
            "nuevo_ct": len(nuevo_data),
            "nuevo_afectados": sum(int(normalize(row['afectados'])) for row in nuevo_data),
            "viejo_ct": len(viejo_data),
            "viejo_afectados": sum(int(normalize(row['afectados'])) for row in viejo_data),
        }
        
        return render_template('comparativa.html', nuevo_data=nuevo_data_with_status, viejo_only=viejo_only, summary=summary)
