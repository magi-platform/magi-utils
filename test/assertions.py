import xml.etree.ElementTree as ElementTree


def verify_prop_values( doc: ElementTree, name: str, value: str ) -> bool:
    matches = False
    for prop in doc.findall( "property" ):
        if prop.find( "name" ).text == name and prop.find( "value" ).text == value:
            print( f'{name} : {prop.find( "value" ).text} : {value}' )
            matches = True
    return matches