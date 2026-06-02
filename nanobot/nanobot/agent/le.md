
 从chat-first到decision-first的范式转移：  
    * 为什么：面向的用户不再是一句话生成简单app的用户，而是长期维护一套复杂系统的开发者，这些开发者靠vibe不可能构建可靠的系统，而古法又太慢，古法配合ai的垂直工作流是靠谱的方式。本质上说是把一个复杂项目按置信度划分成两块:  
    1. 一块是util工具类，common如排序，数据库接入，网络收发，api脚手架和增删改查，或者业务间很确定的关系，这些置信度很高的，ai能直接判断出大概率能做的，帮开发者解决这些。  
    2. 另一块是复杂的具体的业务逻辑，系统状态的转移，字段的取舍，架构的扩展和优化，整体系统的规划这些开发者一开始可能确定不了，是和业务绑定的多变的，ai靠概率不可能精确捕捉的。  
    这俩靠ai区分出来，ai完成第1块，把第二块从完整的项目里剥离出来，形成一个个开发者需要去decision的小task，那么agent系统就成了decision分发器，（整个系统就低熵了）。  
    因为本质上说，一个风格化产品不可能早期几句话就说清楚，开发者必须实时掌控项目架构所有，开发者在开发的过程中的作用，除了编码，还有不断给系统输入信息，去完成系统的整体走向，在软件开发过程中，人的这个外部信息的不断输入的，所以也对应了从系统拆分出来的decisions，人靠decision不断输入。  
    好处是，传统chat-first只有在一轮长的chat结束后才能做简单decision，中间的coding元素是不受控的，而且有很大的回滚的风险和对项目的不确定性。  
    在decision里，一个task被分成细粒度的小task，人始终是在控制项目，并且人一直在做高效的decision，也就是项目本来就需要你做的思考，agent这里是个decision分发器，形成高密度的低延迟的decision。项目是可以由decision组成的。  
    
  传统agent的架构：参考deer-flow，人有在human-loop里，但是参与度太少，粒度太大，风险大，序列模型是question - thinking - answer - human-decision - merge/revert，而这里的架构是持续输入持续decision流，他把chat之后才能进行的human-decision变成了持续的流feed给开发者。  
    架构设计：同时，为了支持高效提取decision，系统支持高并行agent，把项目划分成多个module，module间互相独立，使用接口依赖通信（契约实现，高并行），意味着从传统2层agent架构变成了core agent - module agent - sub agent 3层架构，从而支持互相独立的高并行agent从而产生不断的小decision，当一个decision反馈block时，其他agent的decision又done了，形成了延迟掩盖，于是就有了高并行的agent下的decision高效决策控制台，所以前端才会变成这个样子，他不只是ui的改变，而是范式的转变。  
    系统架构：略
