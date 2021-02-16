/*
 * Copyright (c) 2016, Alexey Romenskiy, All rights reserved.
 *
 * This file is part of fsm
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */

package codes.writeonce.deltastore.schema.xml.reader;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.util.ArrayList;
import java.util.List;

final class XmlUtils {

    @Nullable
    public static String getOptionalText(Node node, XPathExpression expression)
            throws XPathExpressionException, NotUniqueException {
        final Element element = getOptionalElement(node, expression);
        if (element == null) {
            return null;
        }
        return element.getTextContent();
    }

    @Nullable
    public static Element getOptionalElement(Node node, XPathExpression expression)
            throws XPathExpressionException, NotUniqueException {
        final List<Element> elements = getElements(node, expression);
        final int size = elements.size();
        switch (size) {
            case 0:
                return null;
            case 1:
                return elements.get(0);
            default:
                throw new NotUniqueException();
        }
    }

    @Nonnull
    public static List<Element> getElements(Node node, XPathExpression machineExpression)
            throws XPathExpressionException {
        return getNodes((NodeList) machineExpression.evaluate(node, XPathConstants.NODESET), Element.class);
    }

    @Nonnull
    public static <T extends Node> List<T> getNodes(NodeList nodeList, Class<T> type) {
        final int length = nodeList.getLength();
        final List<T> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(type.cast(nodeList.item(i)));
        }
        return list;
    }

    @Nullable
    public static String getOptionalAttribute(Element element, String name) {
        return element.hasAttribute(name) ? element.getAttribute(name) : null;
    }

    @Nonnull
    public static String getRequiredAttribute(Element element, String name) throws NotFoundException {
        if (!element.hasAttribute(name)) {
            throw new NotFoundException();
        }
        return element.getAttribute(name);
    }

    private XmlUtils() {
        // empty
    }
}
