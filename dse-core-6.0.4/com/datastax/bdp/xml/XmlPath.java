package com.datastax.bdp.xml;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Element;

public class XmlPath {
   private static final Joiner SLASH_JOINER = Joiner.on("/");
   private static final Joiner DOT_JOINER = Joiner.on(".");
   private static final Joiner AND_JOINER = Joiner.on(" and ");
   private static final Joiner COMMA_JOINER = Joiner.on(",");
   private static final Splitter SLASH_SPLITTER = Splitter.on("/");
   private static final Splitter COMMA_SPLITTER = Splitter.on(",");
   private static final Pattern ELEMENT_PATTERN = Pattern.compile("([^\\[]*)(\\[(.*)\\])?");
   private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("@([^=]*)='(.*)'");
   private final List<XmlPath.XmlElement> elements;
   private final XPath xpath;

   private XmlPath(List<XmlPath.XmlElement> elements) {
      this.elements = elements;
      this.xpath = XPathFactory.newInstance().newXPath();
   }

   public static XmlPath fromString(String xpath) {
      if(xpath == null) {
         return null;
      } else {
         XmlPath.Builder builder = new XmlPath.Builder();
         SLASH_SPLITTER.split(xpath).forEach((p) -> {
            builder.elements.add(XmlPath.XmlElement.fromString(p.trim()));
         });
         return builder.build();
      }
   }

   public String toExternalPath() {
      StringBuilder pathBuilder = new StringBuilder();
      DOT_JOINER.appendTo(pathBuilder, Iterators.transform(this.elements.iterator(), XmlPath.XmlElement::toExternalPath));
      return pathBuilder.toString();
   }

   public String toXpath() {
      StringBuilder xpathBuilder = new StringBuilder();
      xpathBuilder.append("//");
      SLASH_JOINER.appendTo(xpathBuilder, Iterators.transform(this.elements.iterator(), XmlPath.XmlElement::toXpath));
      return xpathBuilder.toString();
   }

   public String childElementName() {
      return !this.elements.isEmpty()?this.lastElement().name:null;
   }

   public boolean hasMatchingAttribute(String attributeName) {
      return !this.elements.isEmpty()?this.lastElement().attributes.stream().anyMatch((attr) -> {
         return attr.key.equalsIgnoreCase(attributeName);
      }):false;
   }

   public Element buildElementFromParent(Element parent) {
      StringBuilder xpathBuilder = new StringBuilder();
      xpathBuilder.append("/");

      Element child;
      for(Iterator var3 = this.elements.iterator(); var3.hasNext(); parent = child) {
         XmlPath.XmlElement element = (XmlPath.XmlElement)var3.next();
         xpathBuilder.append("/");
         xpathBuilder.append(element.toXpath());
         child = null;

         try {
            child = (Element)this.xpath.evaluate(xpathBuilder.toString(), parent.getOwnerDocument(), XPathConstants.NODE);
         } catch (Exception var8) {
            ;
         }

         if(child == null) {
            child = parent.getOwnerDocument().createElement(element.name);
            Iterator var6 = element.attributes.iterator();

            while(var6.hasNext()) {
               XmlPath.XmlAttribute attribute = (XmlPath.XmlAttribute)var6.next();
               child.setAttribute(attribute.key, attribute.value);
            }

            parent.appendChild(child);
         }
      }

      return parent;
   }

   public String toString() {
      return this.toXpath();
   }

   private XmlPath.XmlElement lastElement() {
      return (XmlPath.XmlElement)this.elements.get(this.elements.size() - 1);
   }

   public static class Builder {
      private final List<XmlPath.XmlElement> elements = new ArrayList();
      private XmlPath.XmlElement currentElement;

      public Builder() {
      }

      public XmlPath.Builder addElement(String elementName) {
         this.currentElement = new XmlPath.XmlElement(elementName);
         this.elements.add(this.currentElement);
         return this;
      }

      public XmlPath.Builder addAttribute(String key, String value) {
         Preconditions.checkNotNull(this.currentElement, "Cannot add an attribute before adding an element");
         this.currentElement.addAttribute(new XmlPath.XmlAttribute(key, value));
         return this;
      }

      public XmlPath build() {
         return new XmlPath(this.elements);
      }
   }

   public static class XmlAttribute {
      private final String key;
      private final String value;

      XmlAttribute(String key, String value) {
         this.key = key;
         this.value = value;
      }

      public static XmlPath.XmlAttribute fromString(String xpath) {
         Matcher m = XmlPath.ATTRIBUTE_PATTERN.matcher(xpath);
         Preconditions.checkArgument(m.find(), xpath + " is not a legal attribute declaration");
         return new XmlPath.XmlAttribute(m.group(1), m.group(2));
      }

      public String toXpath() {
         return String.format("@%s='%s'", new Object[]{this.key, this.value});
      }
   }

   public static class XmlElement {
      private final String name;
      private final List<XmlPath.XmlAttribute> attributes = new ArrayList();

      XmlElement(String name) {
         this.name = name;
      }

      public static XmlPath.XmlElement fromString(String xpath) {
         Matcher m = XmlPath.ELEMENT_PATTERN.matcher(xpath);
         Preconditions.checkArgument(m.find(), xpath + " is not a valid xpath element");
         XmlPath.XmlElement element = new XmlPath.XmlElement(m.group(1));
         if(m.group(3) != null) {
            XmlPath.COMMA_SPLITTER.split(m.group(3)).forEach((a) -> {
               element.attributes.add(XmlPath.XmlAttribute.fromString(a.trim()));
            });
         }

         return element;
      }

      public String toExternalPath() {
         StringBuilder builder = new StringBuilder();
         builder.append(this.name);
         if(!this.attributes.isEmpty()) {
            builder.append("[");
            XmlPath.COMMA_JOINER.appendTo(builder, Iterators.transform(this.attributes.iterator(), XmlPath.XmlAttribute::toXpath));
            builder.append("]");
         }

         return builder.toString();
      }

      public String toXpath() {
         StringBuilder builder = new StringBuilder();
         builder.append(this.name);
         if(!this.attributes.isEmpty()) {
            builder.append("[");
            XmlPath.AND_JOINER.appendTo(builder, Iterators.transform(this.attributes.iterator(), XmlPath.XmlAttribute::toXpath));
            builder.append("]");
         }

         return builder.toString();
      }

      void addAttribute(XmlPath.XmlAttribute attribute) {
         this.attributes.add(attribute);
      }
   }
}
