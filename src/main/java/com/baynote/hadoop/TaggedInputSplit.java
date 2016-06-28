/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.baynote.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An {@link InputSplit} that tags another InputSplit with extra data for use by DelegatingInputFormat's and
 * {@link DelegatingMapper}s.
 * 
 * <p>
 * This package-private class is taken directly from the Hadoop source code, and made public here so that we can
 * leverage it for the {@link com.baynote.kafka.hadoop.MultipleKafkaInputFormat}.
 * 
 * @see DelegatingMapper
 * @see DelegatingRecordReader
 * @see com.baynote.kafka.hadoop.MultipleKafkaInputFormat
 */
public class TaggedInputSplit extends InputSplit implements Configurable, Writable {

    private Class<? extends InputSplit> inputSplitClass;

    private InputSplit inputSplit;

    @SuppressWarnings("unchecked")
    private Class<? extends InputFormat> inputFormatClass;

    @SuppressWarnings("unchecked")
    private Class<? extends Mapper> mapperClass;

    private Configuration conf;

    public TaggedInputSplit() {
        // Default constructor
    }

    /**
     * Creates a new TaggedInputSplit.
     * 
     * @param inputSplit
     *            The InputSplit to be tagged
     * @param conf
     *            The configuration to use
     * @param inputFormatClass
     *            The InputFormat class to use for this job
     * @param mapperClass
     *            The Mapper class to use for this job MultipleM
     */
    @SuppressWarnings("unchecked")
    public TaggedInputSplit(InputSplit inputSplit, Configuration conf, Class<? extends InputFormat> inputFormatClass,
            Class<? extends Mapper> mapperClass) {
        this.inputSplitClass = inputSplit.getClass();
        this.inputSplit = inputSplit;
        this.conf = conf;
        this.inputFormatClass = inputFormatClass;
        this.mapperClass = mapperClass;
    }

    /**
     * Retrieves the original InputSplit.
     * 
     * @return The InputSplit that was tagged
     */
    public InputSplit getInputSplit() {
        return inputSplit;
    }

    /**
     * Retrieves the InputFormat class to use for this split.
     * 
     * @return The InputFormat class to use
     */
    @SuppressWarnings("unchecked")
    public Class<? extends InputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    /**
     * Retrieves the Mapper class to use for this split.
     * 
     * @return The Mapper class to use
     */
    @SuppressWarnings("unchecked")
    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    public long getLength() throws IOException, InterruptedException {
        return inputSplit.getLength();
    }

    public String[] getLocations() throws IOException, InterruptedException {
        return inputSplit.getLocations();
    }

    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        inputSplitClass = (Class<? extends InputSplit>) readClass(in);
        inputFormatClass = (Class<? extends InputFormat<?, ?>>) readClass(in);
        mapperClass = (Class<? extends Mapper<?, ?, ?, ?>>) readClass(in);
        inputSplit = ReflectionUtils.newInstance(inputSplitClass, conf);
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer deserializer = factory.getDeserializer(inputSplitClass);
        deserializer.open((DataInputStream) in);
        inputSplit = (InputSplit) deserializer.deserialize(inputSplit);
    }

    private Class<?> readClass(DataInput in) throws IOException {
        String className = Text.readString(in);
        try {
            return conf.getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("readObject can't find class", e);
        }
    }

    @SuppressWarnings("unchecked")
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, inputSplitClass.getName());
        Text.writeString(out, inputFormatClass.getName());
        Text.writeString(out, mapperClass.getName());
        SerializationFactory factory = new SerializationFactory(conf);
        Serializer serializer = factory.getSerializer(inputSplitClass);
        serializer.open((DataOutputStream) out);
        serializer.serialize(inputSplit);
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
