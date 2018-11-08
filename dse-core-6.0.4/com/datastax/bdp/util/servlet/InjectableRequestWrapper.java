package com.datastax.bdp.util.servlet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.apache.commons.lang3.StringUtils;

public final class InjectableRequestWrapper extends HttpServletRequestWrapper {
   private final Map<String, String[]> parameters;
   private final String queryString;

   public InjectableRequestWrapper(HttpServletRequest request, Map<String, String[]> additionalParameters) {
      super(request);
      Map<String, String[]> originalParameters = super.getParameterMap();
      String originalQueryString = super.getQueryString();
      this.parameters = ImmutableMap.builder().putAll(additionalParameters).putAll(originalParameters).build();
      List<String> parameterStrings = Lists.newArrayListWithExpectedSize(additionalParameters.size() + 1);
      if(StringUtils.isNotBlank(originalQueryString)) {
         parameterStrings.add(originalQueryString);
      }

      Iterator var6 = additionalParameters.entrySet().iterator();

      while(var6.hasNext()) {
         Entry<String, String[]> parameter = (Entry)var6.next();
         String[] var8 = (String[])parameter.getValue();
         int var9 = var8.length;

         for(int var10 = 0; var10 < var9; ++var10) {
            String value = var8[var10];

            try {
               String encoded = URLEncoder.encode(value, "UTF-8");
               parameterStrings.add(String.format("%s=%s", new Object[]{parameter.getKey(), encoded}));
            } catch (UnsupportedEncodingException var13) {
               throw new RuntimeException(var13);
            }
         }
      }

      this.queryString = StringUtils.join(parameterStrings, '&');
   }

   public String getParameter(String name) {
      String[] strings = (String[])this.parameters.get(name);
      return strings != null?strings[0]:super.getParameter(name);
   }

   public Map<String, String[]> getParameterMap() {
      return this.parameters;
   }

   public Enumeration<String> getParameterNames() {
      return Collections.enumeration(this.parameters.keySet());
   }

   public String[] getParameterValues(String name) {
      return (String[])this.parameters.get(name);
   }

   public String getQueryString() {
      return this.queryString;
   }
}
