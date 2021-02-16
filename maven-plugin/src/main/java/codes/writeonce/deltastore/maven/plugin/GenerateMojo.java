package codes.writeonce.deltastore.maven.plugin;

import codes.writeonce.deltastore.schema.generator.SchemaGenerator;
import codes.writeonce.deltastore.schema.xml.reader.ParsingException;
import codes.writeonce.deltastore.schema.xml.reader.SchemaInfo;
import codes.writeonce.deltastore.schema.xml.reader.XmlSchemaReader;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.archiver.MavenArchiveConfiguration;
import org.apache.maven.archiver.MavenArchiver;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.codehaus.plexus.archiver.Archiver;
import org.codehaus.plexus.archiver.jar.JarArchiver;
import org.codehaus.plexus.archiver.jar.ManifestException;
import org.xml.sax.InputSource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.maven.plugins.annotations.LifecyclePhase.GENERATE_SOURCES;

@Mojo(name = "generate", defaultPhase = GENERATE_SOURCES, threadSafe = true)
public class GenerateMojo extends AbstractMojo {

    private static final String DIGEST_ALGORITHM = "SHA-256";
    private static final Charset TEXT_DIGEST_CHARSET = Charset.forName("UTF-8");

    @Parameter(required = true, defaultValue = "${basedir}/src/main/deltastore")
    protected File sourcesBaseDirectory;

    @Parameter(required = true)
    protected List<String> sources;

    @Parameter(required = true, readonly = true, defaultValue = "${project}")
    protected MavenProject project;

    @Parameter(required = true, defaultValue = "${project.build.directory}/generated-sources/deltastore")
    private File javaOutputDirectory;

    @Parameter(defaultValue = "${project.build.sourceEncoding}")
    private String javaOutputCharsetName;

    @Parameter(required = true, readonly = true, defaultValue = "${mojoExecution}")
    private MojoExecution execution;

    @Parameter(required = true, readonly = true, defaultValue = "${project.build.directory}")
    private File buildDirectory;

    @Parameter(required = true, defaultValue = "${project.build.directory}/deltastore-maven-plugin-markers")
    private File markersDirectory;

    @Component(role = Archiver.class, hint = "jar")
    private JarArchiver jarArchiver;

    @Parameter(defaultValue = "${project.build.finalName}", readonly = true)
    private String finalName;

    @Parameter(defaultValue = "${session}", readonly = true, required = true)
    private MavenSession session;

    @Component
    private MavenProjectHelper projectHelper;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            process();
        } catch (MojoExecutionException e) {
            throw e;
        } catch (Exception e) {
            getLog().error(e.getMessage());
            throw new MojoExecutionException("Failed to generate sources: " + e.getMessage(), e);
        }
    }

    protected void process() throws Exception {

        sourcesBaseDirectory = getSafePath(sourcesBaseDirectory);
        javaOutputDirectory = getSafePath(javaOutputDirectory);
        buildDirectory = getSafePath(buildDirectory);
        markersDirectory = getSafePath(markersDirectory);

        final List<Path> files = getSchemaFiles();

        final String artifactFileName = finalName + "-deltastore.jar";

        final byte[] sourceDigestBytes = getSourceDigestBytes();

        final boolean changed = isChanged(sourceDigestBytes, artifactFileName);

        if (changed) {
            getLog().info("Generating Java code from schema configuration.");
            generateJava(files, sourceDigestBytes, artifactFileName);
        } else {
            getLog().info("No changes detected. No Java code generated from schema configuration.");
        }

        project.addCompileSourceRoot(javaOutputDirectory.getPath());
    }

    private void generateJava(
            List<Path> files,
            byte[] sourceDigestBytes,
            String artifactFileName
    ) throws IOException, NoSuchAlgorithmException, ParsingException, ManifestException,
            DependencyResolutionRequiredException {

        final Charset classSourceCharset;
        if (StringUtils.isEmpty(javaOutputCharsetName)) {
            classSourceCharset = Charset.defaultCharset();
            getLog().warn("Using platform encoding (" + classSourceCharset.displayName() +
                          " actually) to generate sources, i.e. build is platform dependent!");
        } else {
            classSourceCharset = Charset.forName(javaOutputCharsetName);
        }

        FileUtils.deleteDirectory(javaOutputDirectory);

        final List<SchemaInfo> schemaInfos = new ArrayList<>();

        final XmlSchemaReader reader = new XmlSchemaReader();
        final ClassLoader classLoader = getProjectClassLoader();

        for (final Path file : files) {
            try (InputStream inputStream = new FileInputStream(file.toFile())) {
                schemaInfos.add(reader.read(new InputSource(inputStream), classLoader));
            }
        }

        Files.createDirectories(javaOutputDirectory.toPath());

        for (final SchemaInfo schemaInfo : schemaInfos) {
            Files.createDirectories(getJavaSourceOutputPath(schemaInfo));
        }

        final SchemaGenerator generator = new SchemaGenerator();

        for (final SchemaInfo schemaInfo : schemaInfos) {
            generator.generate(schemaInfo, getJavaSourceOutputPath(schemaInfo), classSourceCharset);
        }

        final File jarFile = new File(buildDirectory, artifactFileName);
        final MavenArchiver archiver = new MavenArchiver();
        archiver.setArchiver(jarArchiver);
        archiver.setOutputFile(jarFile);
        for (final Path resource : files) {
            final String destFileName = sourcesBaseDirectory.toPath().relativize(resource).toString();
            archiver.getArchiver().addFile(resource.toFile(), destFileName);
        }
        archiver.createArchive(session, project, new MavenArchiveConfiguration());
        projectHelper.attachArtifact(project, "jar", "deltastore", jarFile);

        final byte[] targetDigestBytes = getGeneratedFilesDigest(artifactFileName);

        final Path statusFilePath = getStatusFilePath();
        Files.createDirectories(statusFilePath.getParent());
        try (FileOutputStream out = new FileOutputStream(statusFilePath.toFile());
             OutputStreamWriter outputStreamWriter = new OutputStreamWriter(out, TEXT_DIGEST_CHARSET);
             BufferedWriter writer = new BufferedWriter(outputStreamWriter)) {
            writer.write(Hex.encodeHex(sourceDigestBytes));
            writer.newLine();
            writer.write(Hex.encodeHex(targetDigestBytes));
            writer.newLine();
        }
    }

    private ClassLoader getProjectClassLoader() throws MalformedURLException {

        return URLClassLoader.newInstance(new URL[]{sourcesBaseDirectory.toURI().toURL()},
                Thread.currentThread().getContextClassLoader());
    }

    protected List<Path> getSchemaFiles() throws MojoExecutionException {

        final List<Path> schemaFiles = new ArrayList<>();

        for (final String source : sources) {
            final Path schemaFile = sourcesBaseDirectory.toPath().resolve(source);
            if (!Files.isRegularFile(schemaFile)) {
                throw new MojoExecutionException("Source does not exist: " + source);
            }
            schemaFiles.add(schemaFile);
        }

        return schemaFiles;
    }

    protected File getSafePath(File file) {
        return project.getBasedir().toPath().resolve(file.toPath()).toFile();
    }

    private Path getJavaSourceOutputPath(SchemaInfo schemaInfo) {
        final Path javaOutputPath = javaOutputDirectory.toPath();
        final Path javaPackageSubpath =
                javaOutputPath.getFileSystem().getPath("", schemaInfo.getPackageName().split("\\."));
        return javaOutputPath.resolve(javaPackageSubpath);
    }

    private Path getStatusFilePath() throws NoSuchAlgorithmException {
        final MessageDigest statusDigest = MessageDigest.getInstance(DIGEST_ALGORITHM);
        statusDigest.update(execution.getExecutionId().getBytes(TEXT_DIGEST_CHARSET));
        return markersDirectory.toPath().resolve(Hex.encodeHexString(statusDigest.digest()));
    }

    private boolean isChanged(byte[] sourceDigestBytes, String artifactFileName)
            throws NoSuchAlgorithmException, IOException, DecoderException {

        if (!javaOutputDirectory.exists()) {
            return true;
        }

        if (!Files.exists(buildDirectory.toPath().resolve(artifactFileName))) {
            return true;
        }

        final Path statusFilePath = getStatusFilePath();

        if (!Files.exists(statusFilePath)) {
            return true;
        }

        final byte[] targetDigestBytes = getGeneratedFilesDigest(artifactFileName);

        try (FileInputStream in = new FileInputStream(statusFilePath.toFile());
             InputStreamReader inputStreamReader = new InputStreamReader(in);
             BufferedReader reader = new BufferedReader(inputStreamReader)) {
            return !Arrays.equals(sourceDigestBytes, Hex.decodeHex(reader.readLine().toCharArray())) ||
                   !Arrays.equals(targetDigestBytes, Hex.decodeHex(reader.readLine().toCharArray()));
        }
    }

    private byte[] getSourceDigestBytes() throws NoSuchAlgorithmException, IOException {
        final MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
        for (final String resourcePath : sources) {
            updateDigest(sourcesBaseDirectory.toPath(), md, Paths.get(resourcePath));
        }
        return md.digest();
    }

    private byte[] getGeneratedFilesDigest(String artifactFileName) throws IOException, NoSuchAlgorithmException {
        final MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
        updateDigest(buildDirectory.toPath(), md, Paths.get(artifactFileName));
        final Path root = javaOutputDirectory.toPath();
        final List<Path> files = Utils.getFilesFromSubtree(root);
        Collections.sort(files);
        for (final Path filePath : files) {
            updateDigest(root, md, filePath);
        }
        return md.digest();
    }

    private static void updateDigest(Path root, MessageDigest md, Path filePath) throws IOException {
        md.update(filePath.toString().getBytes(TEXT_DIGEST_CHARSET));
        final Path resolvedFilePath = root.resolve(filePath);
        md.update(Longs.toByteArray(Files.size(resolvedFilePath)));
        md.update(Longs.toByteArray(Files.getLastModifiedTime(resolvedFilePath).toMillis()));
    }
}
