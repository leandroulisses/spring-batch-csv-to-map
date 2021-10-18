/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.leandroulisses.batch.domain;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DefaultFieldSetFactory;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSetFactory;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.beans.PropertyEditor;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A builder implementation for the {@link FlatFileItemReader}.
 *
 * @author Leandro Ulisses dos Passos
 * @since 1.0
 * @see FlatFileItemReader
 */
public class CsvItemReaderBuilder<T> {

	protected Log logger = LogFactory.getLog(getClass());

	private boolean strict = true;

	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;

	private RecordSeparatorPolicy recordSeparatorPolicy = new SimpleRecordSeparatorPolicy();

	private BufferedReaderFactory bufferedReaderFactory = new DefaultBufferedReaderFactory();

	private Resource resource;

	private String delimiter = ",";

	private List<String> comments = new ArrayList<>(Arrays.asList(FlatFileItemReader.DEFAULT_COMMENT_PREFIXES));

	private int linesToSkip = 0;

	private LineCallbackHandler skippedLinesCallback;

	private boolean saveState = true;

	private String name;

	private int maxItemCount = Integer.MAX_VALUE;

	private int currentItemCount;

	/**
	 * Configure if the state of the {@link org.springframework.batch.item.ItemStreamSupport}
	 * should be persisted within the {@link org.springframework.batch.item.ExecutionContext}
	 * for restart purposes.
	 *
	 * @param saveState defaults to true
	 * @return The current instance of the builder.
	 */
	public CsvItemReaderBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;

		return this;
	}

	/**
	 * The name used to calculate the key within the
	 * {@link org.springframework.batch.item.ExecutionContext}. Required if
	 * {@link #saveState(boolean)} is set to true.
	 *
	 * @param name name of the reader instance
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.ItemStreamSupport#setName(String)
	 */
	public CsvItemReaderBuilder<T> name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * Configure the max number of items to be read.
	 *
	 * @param maxItemCount the max items to be read
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setMaxItemCount(int)
	 */
	public CsvItemReaderBuilder<T> maxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;

		return this;
	}

	/**
	 * Index for the current item. Used on restarts to indicate where to start from.
	 *
	 * @param currentItemCount current index
	 * @return this instance for method chaining
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setCurrentItemCount(int)
	 */
	public CsvItemReaderBuilder<T> currentItemCount(int currentItemCount) {
		this.currentItemCount = currentItemCount;

		return this;
	}

	/**
	 * Define the delimiter for the file.
	 *
	 * @param delimiter String used as a delimiter between fields.
	 * @return The instance of the builder for chaining.
	 * @see DelimitedLineTokenizer#setDelimiter(String)
	 */
	public CsvItemReaderBuilder<T> delimiter(String delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	/**
	 * The {@link Resource} to be used as input.
	 *
	 * @param resource the input to the reader.
	 * @return The current instance of the builder.
	 * @see FlatFileItemReader#setResource(Resource)
	 */
	public CsvItemReaderBuilder<T> resource(Resource resource) {
		this.resource = resource;
		return this;
	}

	/**
	 * Configure the encoding used by the reader to read the input source.
	 * Default value is {@link FlatFileItemReader#DEFAULT_CHARSET}.
	 *
	 * @param encoding to use to read the input source.
	 * @return The current instance of the builder.
	 * @see FlatFileItemReader#setEncoding(String)
	 */
	public CsvItemReaderBuilder<T> encoding(String encoding) {
		this.encoding = encoding;
		return this;
	}

	/**
	 * The number of lines to skip at the beginning of reading the file.
	 *
	 * @param linesToSkip number of lines to be skipped.
	 * @return The current instance of the builder.
	 * @see FlatFileItemReader#setLinesToSkip(int)
	 */
	public CsvItemReaderBuilder<T> linesToSkip(int linesToSkip) {
		this.linesToSkip = linesToSkip;
		return this;
	}

	/**
	 * A callback to be called for each line that is skipped.
	 *
	 * @param callback the callback
	 * @return The current instance of the builder.
	 * @see FlatFileItemReader#setSkippedLinesCallback(LineCallbackHandler)
	 */
	public CsvItemReaderBuilder<T> skippedLinesCallback(LineCallbackHandler callback) {
		this.skippedLinesCallback = callback;
		return this;
	}

	/**
	 * Builds the {@link FlatFileItemReader}.
	 *
	 * @return a {@link FlatFileItemReader}
	 */
	public FlatFileItemReader<T> build() {
		if(this.saveState) {
			Assert.state(StringUtils.hasText(this.name),
					"A name is required when saveState is set to true.");
		}

		if(this.resource == null) {
			logger.debug("The resource is null.  This is only a valid scenario when " +
					"injecting it later as in when using the MultiResourceItemReader");
		}

		Assert.notNull(this.recordSeparatorPolicy, "A RecordSeparatorPolicy is required.");
		Assert.notNull(this.bufferedReaderFactory, "A BufferedReaderFactory is required.");

		FlatFileItemReader<T> reader = new FlatFileItemReader<>();

		DefaultLineMapper<T> lineMapper = new DefaultLineMapper<>();
		lineMapper.setFieldSetMapper(new DynamicFieldSetMapper());

		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(generateFromCsv(this.resource));
		tokenizer.setDelimiter(this.delimiter);

		lineMapper.setLineTokenizer(tokenizer);
		reader.setLineMapper(lineMapper);

		if(StringUtils.hasText(this.name)) {
			reader.setName(this.name);
		}

		if(StringUtils.hasText(this.encoding)) {
			reader.setEncoding(this.encoding);
		}

		reader.setResource(this.resource);

		reader.setLinesToSkip(this.linesToSkip);
		reader.setComments(this.comments.toArray(new String[this.comments.size()]));

		reader.setSkippedLinesCallback(this.skippedLinesCallback);
		reader.setRecordSeparatorPolicy(this.recordSeparatorPolicy);
		reader.setBufferedReaderFactory(this.bufferedReaderFactory);
		reader.setMaxItemCount(this.maxItemCount);
		reader.setCurrentItemCount(this.currentItemCount);
		reader.setSaveState(this.saveState);
		reader.setStrict(this.strict);

		return reader;
	}

	private String[] generateFromCsv(Resource resource) {
		try (Stream<String> content = Files.lines(resource.getFile().toPath())){
			String[] headers = content.findFirst()
					.stream()
					.map(lineMapper -> lineMapper.replaceAll("\"", ""))
					.map(lineMapper -> lineMapper.split(this.delimiter))
					.findFirst().orElseThrow(() -> new IllegalStateException("The CSV is empty!"));
			return headers;
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException("Error while processing CSV headers! ", e);
		}
	}

}
