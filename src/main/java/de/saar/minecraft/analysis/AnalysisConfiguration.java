package de.saar.minecraft.analysis;

import java.io.Reader;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class AnalysisConfiguration {

    private String url;
    private String user;
    private String password;
    private String dirName;

    public static AnalysisConfiguration loadYaml(Reader reader) {
        Constructor constructor = new Constructor(AnalysisConfiguration.class);
        Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(reader, AnalysisConfiguration.class);
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDirName() {
        return dirName;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDirName(String dirName) {
        this.dirName = dirName;
    }
}