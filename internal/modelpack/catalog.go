package modelpack

import "slices"

// The catalog: the models this version of Igloo pins, each to an upstream
// revision, its files by SHA-256 and size. `igloo models install name`
// installs one. A model of the catalog that is installed must be the
// catalog's, file for file, to load: a newer Igloo that pins a newer
// revision has it installed again.
var catalog = []Manifest{
	chronosBolt("tiny", "a0e552de83495b5c28c14c71c374f3e33280b340",
		"278f0086733031635fb1c861cb01c1bad6477420c7fcb19381a2993e335785e0", 1120,
		"75068728d376d2bec670379eeef4bfb4d24c0cfe24d957451f8d19b447030a32", 34622352),
	chronosBolt("mini", "251268337516a88e253628c43e1d26ec577b376b",
		"4299e93f30bb8d99d8f7dbaa39a1dfc6bf827ae67d4ef4c0b6ba4ca3df514980", 1120,
		"1a1a4297f132b808c5c7da24e3cce549d519c01ff5c2661fcad404baba018f24", 84956096),
	chronosBolt("small", "772f3d25d38aec6d914c8949dab4462e2d46f5d8",
		"9ca0ebbe049ccebacb0546361e8aa728f0cf104cd18d1838d78d296aa317bd21", 1121,
		"06a6a19bbe74bc10a9cd193bd4bf2bf638ae07f7e0d51653ae7ab8ea968a21dd", 190888824),
	chronosBolt("base", "5d9f166d69f47aef3401367a7b842e78fe97b121",
		"46f7e7e695569f67b06178faa4f236c281392945aa2d1bdf844442e57155530b", 1123,
		"31f875483a3215bc6880a0837ea608a13ce55f88ad90538c3cdd0b29aeb60b36", 821203576),
	{
		Name: "chronos-2", Version: "29ec3766d36d6f73f0696f85560a422f50e8498c", License: "Apache-2.0", Kind: KindForecast,
		Architecture: "chronos-2", Source: "https://huggingface.co/amazon/chronos-2",
		Description: "Amazon Chronos-2, 120M parameters: context up to 8,192 steps, 21 quantiles",
		Files: []File{
			huggingFace("amazon/chronos-2", "29ec3766d36d6f73f0696f85560a422f50e8498c", "config.json",
				"ef1143bfdc9c0376d9a056eefca46cb4b1ec3d0ffacd541ff56feb40fb708031", 1067),
			huggingFace("amazon/chronos-2", "29ec3766d36d6f73f0696f85560a422f50e8498c", "model.safetensors",
				"ddcda3c7508bf2528087723e98a20707cc04b7f370ae275a9fd88078ddba4f42", 477930472),
		},
	},
	{
		Name: "chronos-2-small", Version: "ddec01313e50b6bc58ebaa92ede81bc24a3d9f9a", License: "Apache-2.0", Kind: KindForecast,
		Architecture: "chronos-2", Source: "https://huggingface.co/autogluon/chronos-2-small",
		Description: "Amazon Chronos-2-Small, 28M parameters: context up to 8,192 steps, 13 quantiles",
		Files: []File{
			huggingFace("autogluon/chronos-2-small", "ddec01313e50b6bc58ebaa92ede81bc24a3d9f9a", "config.json",
				"f2780468adc8b16322aa0b6e28b26476c9401ecf64ab53292b6d2b0d3f423722", 969),
			huggingFace("autogluon/chronos-2-small", "ddec01313e50b6bc58ebaa92ede81bc24a3d9f9a", "model.safetensors",
				"492290ae82bb89f9769e3479ce90b3179de1f33e600c34daa0352531538b23cd", 111749048),
		},
	},
	{
		Name: "timesfm-2.5", Version: "1d952420fba87f3c6dee4f240de0f1a0fbc790e3", License: "Apache-2.0", Kind: KindForecast,
		Architecture: "timesfm-2.5", Source: "https://huggingface.co/google/timesfm-2.5-200m-pytorch",
		Description: "Google TimesFM 2.5, 200M parameters (and a 30M quantile head): context up to 16,384 steps, deciles",
		Files: []File{
			huggingFace("google/timesfm-2.5-200m-pytorch", "1d952420fba87f3c6dee4f240de0f1a0fbc790e3", "config.json",
				"cd3315b760d5cc7e278d7afdf41b897031ced888fc6c115bd9b3ac0ea2c47408", 475),
			huggingFace("google/timesfm-2.5-200m-pytorch", "1d952420fba87f3c6dee4f240de0f1a0fbc790e3", "model.safetensors",
				"2f776efe6245e42b24bc4153ffdf61810140210e4bd3b01fb21f7aa779ab6ce8", 925181104),
		},
	},
}

func chronosBolt(size, commit, configSHA string, configSize int64, weightsSHA string, weightsSize int64) Manifest {
	repo := "amazon/chronos-bolt-" + size
	return Manifest{
		Name: "chronos-bolt-" + size, Version: commit, License: "Apache-2.0", Kind: KindForecast, Architecture: "chronos-bolt",
		Source: "https://huggingface.co/" + repo, Description: "Amazon Chronos-Bolt (" + size + "): context up to 2,048 steps, deciles",
		Files: []File{
			huggingFace(repo, commit, "config.json", configSHA, configSize),
			huggingFace(repo, commit, "model.safetensors", weightsSHA, weightsSize),
		},
	}
}

// Catalog returns the models this version of Igloo pins.
func Catalog() []Manifest { return slices.Clone(catalog) }

// Lookup returns the catalog's model name.
func Lookup(name string) (Manifest, bool) {
	for _, m := range catalog {
		if m.Name == name {
			return m, true
		}
	}
	return Manifest{}, false
}
