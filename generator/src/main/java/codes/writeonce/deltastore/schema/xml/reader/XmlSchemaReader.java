package codes.writeonce.deltastore.schema.xml.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.bind.DatatypeConverter;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static codes.writeonce.deltastore.schema.xml.reader.XmlUtils.getElements;
import static codes.writeonce.deltastore.schema.xml.reader.XmlUtils.getOptionalAttribute;
import static codes.writeonce.deltastore.schema.xml.reader.XmlUtils.getRequiredAttribute;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public class XmlSchemaReader {

    private static final Schema SCHEMA;

    static {
        try {
            final SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            SCHEMA = sf.newSchema(requireNonNull(XmlSchemaReader.class.getClassLoader().getResource(
                    "codes/writeonce/deltastore/schema/xml/deltastore.xsd")));
        } catch (SAXException e) {
            throw new RuntimeException("XML config reader initialization failed", e);
        }
    }

    private static final String NAMESPACE = "http://writeonce.codes/xsd/deltastore";

    private static final NamespaceContextImpl NAMESPACE_CONTEXT = new NamespaceContextImpl();

    private static final ErrorHandlerImpl ERROR_HANDLER = new ErrorHandlerImpl();

    private final Map<String, SchemaInfo> resourcePathToSchemaInfoMap = new HashMap<>();

    private final XPathExpression importExpression;
    private final XPathExpression typeExpression;
    private final XPathExpression fieldExpression;
    private final XPathExpression extendsExpression;
    private final XPathExpression keyExpression;

    {
        try {
            final XPathFactory xpf = XPathFactory.newInstance();
            final XPath xp = xpf.newXPath();
            xp.setNamespaceContext(NAMESPACE_CONTEXT);
            importExpression = xp.compile("n:import");
            typeExpression = xp.compile("n:type");
            fieldExpression = xp.compile("n:field");
            extendsExpression = xp.compile("n:extends");
            keyExpression = xp.compile("n:key");
        } catch (XPathExpressionException e) {
            throw new RuntimeException("XML config reader instance initialization failed", e);
        }
    }

    public SchemaInfo read(InputSource inputSource, ClassLoader classLoader) throws ParsingException {
        try {
            final Element documentElement = parseDocument(inputSource).getDocumentElement();

            final String schemaName = getRequiredAttribute(documentElement, "name");
            final String packageName = getRequiredAttribute(documentElement, "package");
            final boolean schemaInstantiable = !getOptionalBoolean(documentElement, "abstract", false);

            final LinkedHashMap<String, EntityTypeInfo> typeMap = new LinkedHashMap<>();
            final LinkedHashMap<String, KeyInfo> keyMap = new LinkedHashMap<>();
            final LinkedHashMap<String, SchemaInfo> parentSchemaMap = new LinkedHashMap<>();
            final LinkedHashMap<String, SchemaInfo> directParentSchemaMap = new LinkedHashMap<>();

            for (final Element importElement : getElements(documentElement, importExpression)) {
                final String resourcePath = getRequiredAttribute(importElement, "resource");
                final SchemaInfo schemaInfo = getSchemaInfo(classLoader, resourcePath);
                typeMap.putAll(schemaInfo.getTypeMap());
                keyMap.putAll(schemaInfo.getKeyMap());
                parentSchemaMap.putAll(schemaInfo.getParentSchemaMap());
                parentSchemaMap.put(schemaInfo.getName(), schemaInfo);
                directParentSchemaMap.put(schemaInfo.getName(), schemaInfo);
            }

            for (final Element typeElement : getElements(documentElement, typeExpression)) {

                final LinkedHashSet<String> parents = new LinkedHashSet<>();
                final LinkedHashMap<String, FieldInfo> fieldMap = new LinkedHashMap<>();
                final LinkedHashMap<String, KeyInfo> entityKeyMap = new LinkedHashMap<>();

                final String name = getRequiredAttribute(typeElement, "name");
                final boolean instantiable = !getOptionalBoolean(typeElement, "abstract", false);
                final String key = getOptionalAttribute(typeElement, "key");
                ofNullable(getOptionalAttribute(typeElement, "extends")).ifPresent(parents::add);

                for (final Element extendsElement : getElements(typeElement, extendsExpression)) {
                    parents.add(getRequiredAttribute(extendsElement, "ref"));
                }

                final EntityTypeInfo entityTypeInfo =
                        new EntityTypeInfo(schemaName, parents, name, instantiable, key, fieldMap, entityKeyMap);
                typeMap.put(name, entityTypeInfo);

                for (final Element fieldElement : getElements(typeElement, fieldExpression)) {
                    final String fieldName = getRequiredAttribute(fieldElement, "name");
                    final boolean mutable = getOptionalBoolean(fieldElement, "mutable", true);
                    final boolean nullable = getOptionalBoolean(fieldElement, "nullable", true);
                    final String type = getRequiredAttribute(fieldElement, "type");
                    final FieldInfo fieldInfo;
                    switch (type) {
                        case "integer":
                            fieldInfo = new IntegerFieldInfo(fieldName, mutable, nullable, entityTypeInfo);
                            break;
                        case "long":
                            fieldInfo = new LongFieldInfo(fieldName, mutable, nullable, entityTypeInfo);
                            break;
                        case "timestamp":
                            fieldInfo = new InstantFieldInfo(fieldName, mutable, nullable, entityTypeInfo);
                            break;
                        case "boolean":
                            fieldInfo = new BooleanFieldInfo(fieldName, mutable, nullable, entityTypeInfo);
                            break;
                        case "decimal":
                            fieldInfo = new BigDecimalFieldInfo(fieldName, mutable, nullable, entityTypeInfo);
                            break;
                        case "string":
                            fieldInfo = new StringFieldInfo(fieldName, mutable, nullable, entityTypeInfo);
                            break;
                        case "enum":
                            final String enumType = getRequiredAttribute(fieldElement, "enumType");
                            fieldInfo = new EnumFieldInfo(fieldName, mutable, nullable, entityTypeInfo, enumType);
                            break;
                        case "id":
                            final String idType = getRequiredAttribute(fieldElement, "idType");
                            fieldInfo = new IdFieldInfo(fieldName, mutable, nullable, entityTypeInfo, idType);
                            break;
                        default:
                            throw new UnsupportedFieldTypeException("Unsupported field type: " + type);
                    }
                    fieldMap.put(fieldName, fieldInfo);
                }

                for (final Element keyElement : getElements(typeElement, keyExpression)) {

                    final String keyName = getRequiredAttribute(keyElement, "name");
                    final boolean unique = getOptionalBoolean(keyElement, "unique", false);
                    final LinkedHashSet<String> keyFields = new LinkedHashSet<>();

                    for (final Element fieldElement : getElements(keyElement, fieldExpression)) {
                        keyFields.add(getRequiredAttribute(fieldElement, "ref"));
                    }

                    final KeyInfo keyInfo = new KeyInfo(schemaName, keyName, unique, keyFields, entityTypeInfo);
                    keyMap.put(keyName, keyInfo);
                    entityKeyMap.put(keyName, keyInfo);
                }
            }

            return new SchemaInfo(schemaName, packageName, schemaInstantiable, typeMap, keyMap, parentSchemaMap,
                    directParentSchemaMap);
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException(e);
        }
    }

    private SchemaInfo getSchemaInfo(ClassLoader classLoader, String resourcePath)
            throws IOException, ParsingException {

        final SchemaInfo existingSchemaInfo = resourcePathToSchemaInfoMap.get(resourcePath);

        if (existingSchemaInfo == null) {
            final List<URL> resources = Collections.list(classLoader.getResources(resourcePath));
            if (resources.isEmpty()) {
                throw new ParsingException("Source does not exist: " + resourcePath);
            }
            if (resources.size() > 1) {
                throw new ParsingException("Multiple sources for the path \"" + resourcePath + "\": " + resources);
            }
            final URL resource = resources.get(0);
            try (InputStream resourceStream = resource.openStream()) {
                final SchemaInfo schemaInfo = read(new InputSource(resourceStream), classLoader);
                resourcePathToSchemaInfoMap.put(resourcePath, schemaInfo);
                return schemaInfo;
            }
        } else {
            return existingSchemaInfo;
        }
    }

    private boolean getOptionalBoolean(Element element, String name, boolean defaultValue) {
        return ofNullable(getOptionalAttribute(element, name)).map(DatatypeConverter::parseBoolean)
                .orElse(defaultValue);
    }

    private Document parseDocument(InputSource inputSource)
            throws ParserConfigurationException, SAXException, IOException {

        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setSchema(SCHEMA);
        dbf.setXIncludeAware(true);

        final DocumentBuilder db = dbf.newDocumentBuilder();
        db.setErrorHandler(ERROR_HANDLER);
        return db.parse(inputSource);
    }

    private static class NamespaceContextImpl implements NamespaceContext {

        @Override
        public String getNamespaceURI(String prefix) {
            if (prefix == null) {
                throw new IllegalArgumentException();
            }
            if ("n".equals(prefix)) {
                return NAMESPACE;
            }
            return XMLConstants.NULL_NS_URI;
        }

        @Override
        public String getPrefix(String namespaceURI) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<String> getPrefixes(String namespaceURI) {
            throw new UnsupportedOperationException();
        }
    }

    private static class ErrorHandlerImpl implements ErrorHandler {

        protected final Logger log = LoggerFactory.getLogger(getClass());

        @Override
        public void warning(SAXParseException exception) {
            log.warn(exception.getMessage(), exception);
        }

        @Override
        public void error(SAXParseException exception) throws SAXException {
            throw exception;
        }

        @Override
        public void fatalError(SAXParseException exception) throws SAXException {
            throw exception;
        }
    }
}
