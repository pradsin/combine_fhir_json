# example
import pyCCDA

with open(r"""C:\Users\prads\source\repos\healthwizz\DrVandana\Data\CCDA_ERROR\CCDA0000986.xml""") as f:
    ccd = pyCCDA.CCDA(f.read())
    # ccd.type   # The document type ('ccda', 'c32', and such)
    # ccd.source # The parsed source data (XML) with added querying methods
    # ccd.data   # The final parsed document data
name = ccd.data.demographics.name
print(name.prefix, name.given, name.family)
