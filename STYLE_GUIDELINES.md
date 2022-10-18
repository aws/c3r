# Style Guidelines
Thank you for your interest in contributing to our project. We greatly value feedback and contributions from our community. 
Please read through this document before submitting any pull requests to ensure all code requirements have been met so 
we can effectively respond to your contribution.

## Automated Verification
The core rules that must be followed are automatically enforced by Checkstyle (see 
[checkstyle.xml](config/checkstyle/checkstyle.xml) for specific rules). This is run as part of the build process on GitHub
automatically and any failures will need to be fixed. Beyond that, we have a number of best practices and guidelines to 
follow.

## Best Practices
### Names
Class, parameter, and variable names should be descriptive of what they contain. Except for iterators inside `for` loops, single character 
names should be avoided. By looking at the method signature, another developer should be able to have a general idea of 
what the method is doing and what each parameter contains without having to look at surrounding code.

### Encapsulation for Type Safety
Type safety is one of the cornerstones of security in programming languages. It's what prevents you from using an integer
as a pointer or indexing off the end of an array. Java was designed with type safety in mind. At compile time, a static 
byte code verifier is run to ensure correct type information in as many locations as possible. The points it can't verify
are turned into dynamic checks at runtime using the class tag that exists in memory with every object.

To reduce the risk of developer errors, we encapsulate our data types, even if it is a single string (see 
[ColumnHeader](c3r-sdk-core/src/main/java/com/amazonaws/c3r/config/ColumnHeader.java) as an example).

### Unchecked Exceptions
As a practice, we do not include unchecked exceptions (`java.lang.RuntimeException` and any subclasses) as part of the 
method or constructor's `throws` clause which is 
[standard practice](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/RuntimeException.html). They
are included as part of the Javadoc for the method or constructor, however. Checked exceptions should always be part of 
the `throws` clause and the Javadoc.

### Javadocs
All non-test Java code requires a Javadoc, regardless of the visibility. Checkstyle enforces the majority of the rules 
around what must be in a Javadoc but there are several style preferences:
  - `{@code text}` is preferred over `<code>text</code>` for readability purposes
  - When listing a set of requirements or specifications, use `<ul>` and `<li>` tags to create a list when the document 
    is generated. If you need to nest lists the proper format is:
    ````
    <ul>
      <li>Item 1</li>
      <li>Item 2</li>
      <li>
        <ul>
          <li>Child 1 of Item 2</li>
          <li>Child 2 of Item 2</li>
        </ul>
      </li>
    </ul>
    ````
    All items in the list should be consistent in whether they end in periods.
  - Use of `{@link}` should be minimized and only included when it's useful.
  - `@see` tags should be ordered from closest to furthest code location (method in same class, class in same package, 
    class in another package, etc.)
  - Only include parenthesis or parameter information in a `@link` or `@see` to a function if it's needed to distinguish 
    between two methods of the same name.
  - The descriptions follow all tags should be short and not end with a period.
  - For `@return` we reference the primary return value first and the default second:
    - `@return {@code true} if there is more data, else {@code false}`
    - `@return The list of column names in the schema or {@code null} if none are specified`
  - For `@throws` we use the format `@throws <Exception> If <condition>`.

If in doubt on the style for a Javadoc, we follow the standard 
[Oracle model](https://www.oracle.com/technical-resources/articles/java/javadoc-tool.html).