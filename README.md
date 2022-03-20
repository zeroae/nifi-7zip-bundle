# nifi-7zip-bundle
NiFi Processors based on [7zipBinding](http://sevenzipjbind.sourceforge.net)

## Usage
```
mvn clean install
cp nifi-7zip-nar/target/*.nar $NIFI_HOME/extensions
```

## TODO
- Support [multi-part unpacking](http://sevenzipjbind.sourceforge.net/extraction_snippets.html#open-multipart-archives)
- Implement [unpacking callback](http://sevenzipjbind.sourceforge.net/extraction_snippets.html#extracting-archive-std-int-callback)
- Convert `mime.type` to [7ZipBinding ArchiveFormat](http://sevenzipjbind.sourceforge.net/javadoc/net/sf/sevenzipjbinding/ArchiveFormat.html)

---
