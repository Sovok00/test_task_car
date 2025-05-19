# remove_bom.py
with open('cars_20250518.csv', 'rb') as f_in:  # Открываем в бинарном режиме
    content = f_in.read()
    # Удаляем BOM для UTF-16 LE/BE или UTF-8
    for bom in [b'\xff\xfe', b'\xfe\xff', b'\xef\xbb\xbf']:
        if content.startswith(bom):
            content = content[len(bom):]
            break
with open('cars_clean.csv', 'wb') as f_out:  # Записываем в бинарном режиме
    f_out.write(content)