# This is a powershell script that assembles compiled Spark for distribution

$FWDIR=split-path -parent $MyInvocation.MyCommand.Definition
$DISTDIR="$FWDIR/dist"

# Make directories
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "$DISTDIR"
New-Item -Path "$DISTDIR/lib" -ItemType directory
echo "Spark $VERSION built for Hadoop $SPARK_HADOOP_VERSION" > "$DISTDIR/RELEASE"

# Copy jars
cp $FWDIR\assembly\target\scala*\*assembly*hadoop*.jar "$DISTDIR\lib\"
cp $FWDIR\examples\target\scala*\spark-examples*.jar "$DISTDIR\lib\"
cp $FWDIR\network\yarn\target\scala*\spark-*-yarn-shuffle.jar "$DISTDIR\lib\" #-Erroraction 'silentlycontinue'
cp $FWDIR\network\yarn\target\scala*\spark-network-yarn*.jar "$DISTDIR\lib\"
cp $FWDIR\lib_managed\jars\datanucleus*.jar "$DISTDIR\lib\"
#cp $FWDIR\network\yarn\target\scala*\spark-network-shuffle*.jar "$DISTDIR\lib\"
#cp $FWDIR\network\yarn\target\scala*\spark-network-common*.jar "$DISTDIR\lib\"


# Copy example sources (needed for python and SQL)
New-Item -Path "$DISTDIR/examples/src" -ItemType directory
cp -r "$FWDIR/examples/src/main" "$DISTDIR/examples/src/" 


# Copy license, ASF files and getting started files
cp "$FWDIR/LICENSE" "$DISTDIR"
cp "$FWDIR/NOTICE" "$DISTDIR"
if (Test-Path "$FWDIR/CHANGES.txt" ) { cp "$FWDIR/CHANGES.txt" "$DISTDIR" } 

# Copy other things
New-Item -Path "$DISTDIR/conf" -ItemType directory
cp $FWDIR/conf/*.template "$DISTDIR/conf"
cp "$FWDIR/README.md" "$DISTDIR"
cp -r "$FWDIR/bin" "$DISTDIR"
cp -r "$FWDIR/python" "$DISTDIR"
cp -r "$FWDIR/sbin" "$DISTDIR"
cp -r "$FWDIR/ec2" "$DISTDIR"
